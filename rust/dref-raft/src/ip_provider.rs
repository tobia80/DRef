//! Node address discovery for Raft clusters.
//!
//! Mirrors the Scala [`IpProvider`] trait: resolve peer IPs from Kubernetes
//! Endpoints, static lists, DNS names, or localhost. Used when
//! [`RaftConfig::initial_endpoints`] is empty and the standard
//! `DREF_*` environment variables are set.

use std::collections::HashSet;
use std::env;
use std::sync::Arc;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Endpoints;
use kube::{Api, Client};

use crate::config::NodeEndpoint;

/// Errors while resolving node addresses.
#[derive(Debug, thiserror::Error)]
pub enum IpProviderError {
    #[error("kubernetes API error: {0}")]
    Kubernetes(String),
    #[error("DNS resolution error: {0}")]
    Dns(String),
    #[error("no local address could be determined")]
    NoLocalAddress,
}

/// Resolves the IP addresses of every Raft peer in the cluster.
#[async_trait]
pub trait IpProvider: Send + Sync {
    async fn find_node_addresses(&self) -> Result<Vec<String>, IpProviderError>;

    async fn find_my_address(&self) -> Result<String, IpProviderError>;

    async fn expected_endpoints(&self) -> Result<usize, IpProviderError> {
        self.find_node_addresses().await.map(|ips| ips.len())
    }
}

/// Localhost-only discovery (single-node / tests).
pub struct LocalIpProvider;

#[async_trait]
impl IpProvider for LocalIpProvider {
    async fn find_node_addresses(&self) -> Result<Vec<String>, IpProviderError> {
        Ok(vec!["127.0.0.1".to_string()])
    }

    async fn find_my_address(&self) -> Result<String, IpProviderError> {
        Ok("127.0.0.1".to_string())
    }

    async fn expected_endpoints(&self) -> Result<usize, IpProviderError> {
        Ok(1)
    }
}

/// Fixed list of peer IPs.
pub struct StaticIpProvider {
    ips: Vec<String>,
}

impl StaticIpProvider {
    pub fn new(ips: Vec<String>) -> Self {
        Self { ips }
    }
}

#[async_trait]
impl IpProvider for StaticIpProvider {
    async fn find_node_addresses(&self) -> Result<Vec<String>, IpProviderError> {
        Ok(self.ips.clone())
    }

    async fn find_my_address(&self) -> Result<String, IpProviderError> {
        let local = local_interface_ips();
        if let Some(ip) = self.ips.iter().find(|ip| local.contains(*ip)) {
            return Ok(ip.clone());
        }
        hostname_fallback()
    }

    async fn expected_endpoints(&self) -> Result<usize, IpProviderError> {
        Ok(self.ips.len())
    }
}

/// DNS-based discovery (Docker Compose service names, headless services, etc.).
pub struct DnsIpProvider {
    services: Vec<String>,
}

impl DnsIpProvider {
    pub fn new(services: Vec<String>) -> Self {
        Self { services }
    }
}

#[async_trait]
impl IpProvider for DnsIpProvider {
    async fn find_node_addresses(&self) -> Result<Vec<String>, IpProviderError> {
        let mut ips = Vec::new();
        for service in &self.services {
            let addrs = tokio::net::lookup_host(format!("{service}:0"))
                .await
                .map_err(|e| IpProviderError::Dns(format!("{service}: {e}")))?;
            for addr in addrs {
                ips.push(addr.ip().to_string());
            }
        }
        Ok(ips)
    }

    async fn find_my_address(&self) -> Result<String, IpProviderError> {
        let endpoint_ips = self.find_node_addresses().await?;
        pick_local_address(&endpoint_ips)
    }
}

/// Kubernetes Endpoints-based discovery (`DREF_K8S_SERVICE` + `DREF_K8S_NAMESPACE`).
pub struct KubernetesIpProvider {
    client: Client,
    service_name: String,
    namespace: String,
}

impl KubernetesIpProvider {
    pub async fn new(service_name: String, namespace: String) -> Result<Self, IpProviderError> {
        let client = Client::try_default()
            .await
            .map_err(|e| IpProviderError::Kubernetes(e.to_string()))?;
        Ok(Self {
            client,
            service_name,
            namespace,
        })
    }
}

#[async_trait]
impl IpProvider for KubernetesIpProvider {
    async fn find_node_addresses(&self) -> Result<Vec<String>, IpProviderError> {
        let api: Api<Endpoints> = Api::namespaced(self.client.clone(), &self.namespace);
        let ep = api
            .get(&self.service_name)
            .await
            .map_err(|e| IpProviderError::Kubernetes(e.to_string()))?;
        Ok(extract_endpoint_ips(&ep))
    }

    async fn find_my_address(&self) -> Result<String, IpProviderError> {
        let endpoint_ips = self.find_node_addresses().await?;
        pick_local_address(&endpoint_ips)
    }
}

/// Extract ready pod IPs from a Kubernetes `Endpoints` object.
pub fn extract_endpoint_ips(endpoints: &Endpoints) -> Vec<String> {
    endpoints
        .subsets
        .as_ref()
        .map(|subsets| {
            subsets
                .iter()
                .flat_map(|subset| {
                    subset
                        .addresses
                        .as_ref()
                        .map(|addrs| addrs.iter().map(|a| a.ip.clone()).collect::<Vec<_>>())
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Read `DREF_PORT` or fall back to the given default.
pub fn port_from_env(default: u16) -> u16 {
    env::var("DREF_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Build an [`IpProvider`] from the standard `DREF_*` environment variables.
///
/// Priority matches the Scala example app:
/// 1. `DREF_K8S_SERVICE` + `DREF_K8S_NAMESPACE`
/// 2. `DREF_NODE_ADDRESSES`
/// 3. `DREF_NODE_SERVICES`
pub async fn from_env() -> Result<Option<Arc<dyn IpProvider>>, IpProviderError> {
    if let (Some(service), Some(namespace)) = (
        env::var("DREF_K8S_SERVICE").ok(),
        env::var("DREF_K8S_NAMESPACE").ok(),
    ) {
        let provider = KubernetesIpProvider::new(service, namespace).await?;
        return Ok(Some(Arc::new(provider)));
    }

    if let Ok(raw) = env::var("DREF_NODE_ADDRESSES") {
        let ips = parse_csv(&raw);
        if !ips.is_empty() {
            return Ok(Some(Arc::new(StaticIpProvider::new(ips))));
        }
    }

    if let Ok(raw) = env::var("DREF_NODE_SERVICES") {
        let services = parse_csv(&raw);
        if !services.is_empty() {
            return Ok(Some(Arc::new(DnsIpProvider::new(services))));
        }
    }

    Ok(None)
}

/// Turn peer IPs into gRPC [`NodeEndpoint`]s (IP used as provisional node id).
pub fn node_endpoints_from_ips(ips: &[String], port: u16) -> Vec<NodeEndpoint> {
    ips.iter()
        .map(|ip| NodeEndpoint::new(ip.clone(), format!("{ip}:{port}")))
        .collect()
}

fn local_interface_ips() -> HashSet<String> {
    let mut ips = HashSet::new();
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if iface.is_loopback() {
                continue;
            }
            ips.insert(iface.ip().to_string());
        }
    }
    ips
}

fn pick_local_address(endpoint_ips: &[String]) -> Result<String, IpProviderError> {
    let local = local_interface_ips();
    if let Some(ip) = endpoint_ips.iter().find(|ip| local.contains(*ip)) {
        return Ok(ip.clone());
    }
    hostname_fallback()
}

fn hostname_fallback() -> Result<String, IpProviderError> {
    let name = hostname::get().map_err(|_| IpProviderError::NoLocalAddress)?;
    let host = name.to_string_lossy();
    let query = (host.as_ref(), 0);
    std::net::ToSocketAddrs::to_socket_addrs(&query)
        .ok()
        .and_then(|mut iter| iter.next())
        .map(|addr| addr.ip().to_string())
        .ok_or(IpProviderError::NoLocalAddress)
}

#[cfg(test)]
mod tests {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    use super::*;

    fn make_endpoints(name: &str, ips: &[&str]) -> Endpoints {
        Endpoints {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            subsets: Some(vec![k8s_openapi::api::core::v1::EndpointSubset {
                addresses: Some(
                    ips.iter()
                        .map(|ip| k8s_openapi::api::core::v1::EndpointAddress {
                            ip: ip.to_string(),
                            ..Default::default()
                        })
                        .collect(),
                ),
                ..Default::default()
            }]),
            ..Default::default()
        }
    }

    #[test]
    fn extract_endpoint_ips_reads_all_addresses() {
        let ep = make_endpoints("my-service", &["10.0.0.1", "10.0.0.2", "10.0.0.3"]);
        assert_eq!(
            extract_endpoint_ips(&ep),
            vec!["10.0.0.1", "10.0.0.2", "10.0.0.3"]
        );
    }

    #[test]
    fn extract_endpoint_ips_handles_multiple_subsets() {
        let ep = Endpoints {
            metadata: ObjectMeta {
                name: Some("multi".to_string()),
                ..Default::default()
            },
            subsets: Some(vec![
                k8s_openapi::api::core::v1::EndpointSubset {
                    addresses: Some(vec![
                        k8s_openapi::api::core::v1::EndpointAddress {
                            ip: "10.0.1.1".to_string(),
                            ..Default::default()
                        },
                        k8s_openapi::api::core::v1::EndpointAddress {
                            ip: "10.0.1.2".to_string(),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                },
                k8s_openapi::api::core::v1::EndpointSubset {
                    addresses: Some(vec![k8s_openapi::api::core::v1::EndpointAddress {
                        ip: "10.0.2.1".to_string(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        assert_eq!(
            extract_endpoint_ips(&ep),
            vec!["10.0.1.1", "10.0.1.2", "10.0.2.1"]
        );
    }

    #[test]
    fn extract_endpoint_ips_empty_when_no_subsets() {
        let ep = Endpoints {
            metadata: ObjectMeta {
                name: Some("empty".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(extract_endpoint_ips(&ep).is_empty());
    }

    #[tokio::test]
    async fn static_provider_returns_configured_ips() {
        let provider = StaticIpProvider::new(vec!["10.0.0.1".into(), "10.0.0.2".into()]);
        assert_eq!(
            provider.find_node_addresses().await.unwrap(),
            vec!["10.0.0.1", "10.0.0.2"]
        );
        assert_eq!(provider.expected_endpoints().await.unwrap(), 2);
    }

    #[test]
    fn node_endpoints_from_ips_formats_host_port() {
        let eps = node_endpoints_from_ips(&["10.0.0.5".into()], 8082);
        assert_eq!(eps.len(), 1);
        assert_eq!(eps[0].id, "10.0.0.5");
        assert_eq!(eps[0].address, "10.0.0.5:8082");
    }
}
