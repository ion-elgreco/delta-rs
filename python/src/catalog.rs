//! Catalogs that open the tables that they know by name

use std::sync::Arc;

use deltalake::DeltaTableError;
use deltalake_catalog_unity::{
    TokenFuture, TokenProvider, UnityCatalog, UnityCatalogBuilder, UnityCatalogConfigKey,
    UnityCatalogError,
};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::error::PythonError;

/// A Unity Catalog server
#[pyclass(module = "deltalake._internal", frozen)]
pub struct RawUnityCatalog {
    pub(crate) inner: UnityCatalog,
}

#[pymethods]
impl RawUnityCatalog {
    #[new]
    #[pyo3(signature = (workspace_url, token = None))]
    fn new(workspace_url: &str, token: Option<Bound<'_, PyAny>>) -> PyResult<Self> {
        let mut builder = UnityCatalogBuilder::from_env()
            .try_with_option(UnityCatalogConfigKey::Host, workspace_url)
            .map_err(catalog_error)?;
        if let Some(token) = token {
            if let Ok(token) = token.extract::<String>() {
                builder = builder
                    .try_with_option(UnityCatalogConfigKey::Token, token)
                    .map_err(catalog_error)?;
            } else if token.is_callable() {
                let token_provider = PyTokenProvider(Arc::new(token.unbind()));
                builder = builder.with_token_provider(Arc::new(token_provider));
            } else {
                return Err(PyTypeError::new_err(
                    "token must be a str or a callable without arguments that returns a str",
                ));
            }
        }
        Ok(Self {
            inner: builder.build().map_err(catalog_error)?,
        })
    }
}

fn catalog_error(error: impl Into<UnityCatalogError>) -> PyErr {
    PythonError::from(DeltaTableError::from(error.into())).into()
}

/// A callable of Python that returns the access token.
struct PyTokenProvider(Arc<Py<PyAny>>);

impl TokenProvider for PyTokenProvider {
    fn token(&self) -> TokenFuture<'_> {
        let callable = self.0.clone();
        Box::pin(async move {
            // The callable can block, and it needs the GIL.
            let token = tokio::task::spawn_blocking(move || {
                Python::attach(|py| {
                    callable
                        .bind(py)
                        .call0()
                        .and_then(|token| token.extract::<String>())
                        .map_err(|error| format!("the token callable failed: {error}"))
                })
            })
            .await??;
            Ok(token)
        })
    }
}
