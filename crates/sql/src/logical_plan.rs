use std::cmp::Ordering;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef, DataFusionError, TableReference};
use datafusion::logical_expr::logical_plan::LogicalPlan;
use datafusion::logical_expr::{Expr, UserDefinedLogicalNodeCore};

/// Delta Lake specific operations
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum DeltaStatement {
    /// Get provenance information, including the operation,
    /// user, and so on, for each write to a table.
    DescribeHistory(DescribeHistory),
    DescribeDetails(DescribeDetails),
    DescribeFiles(DescribeFiles),
    /// Remove unused files from a table directory.
    Vacuum(Vacuum),
}

impl Debug for DeltaStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

impl DeltaStatement {
    /// Return a `format`ed structure with a human-readable
    /// description of this LogicalPlan node per node, not including
    /// children.
    pub fn display(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a DeltaStatement);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.0 {
                    &DeltaStatement::Vacuum(Vacuum {
                        ref table,
                        ref dry_run,
                        ref retention_hours,
                        ..
                    }) => {
                        if let Some(ret) = retention_hours {
                            write!(f, "Vacuum: {table} retention_hours={ret} dry_run={dry_run}")
                        } else {
                            write!(f, "Vacuum: {table} dry_run={dry_run}")
                        }
                    }
                    DeltaStatement::DescribeHistory(DescribeHistory { table, .. }) => {
                        write!(f, "DescribeHistory: {table:?}")
                    }
                    DeltaStatement::DescribeDetails(DescribeDetails { table, .. }) => {
                        write!(f, "DescribeDetails: {table:?}")
                    }
                    DeltaStatement::DescribeFiles(DescribeFiles { table, .. }) => {
                        write!(f, "DescribeFiles: {table:?}")
                    }
                }
            }
        }
        Wrapper(self)
    }
}

impl UserDefinedLogicalNodeCore for DeltaStatement {
    fn name(&self) -> &str {
        match self {
            Self::DescribeDetails(_) => "DescribeDetails",
            Self::DescribeHistory(_) => "DescribeHistory",
            Self::DescribeFiles(_) => "DescribeFiles",
            Self::Vacuum(_) => "Vacuum",
        }
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        match self {
            Self::Vacuum(Vacuum { schema, .. }) => schema,
            _ => todo!(),
        }
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        match self {
            Self::Vacuum(_) | Self::DescribeHistory(_) => {
                if !inputs.is_empty() {
                    return Err(DataFusionError::External("Input size inconsistent".into()));
                }
                if !exprs.is_empty() {
                    return Err(DataFusionError::External(
                        "Expression size inconsistent".into(),
                    ));
                }
                Ok(self.clone())
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "with_exprs_and_inputs not implemented for {self:?}",
            ))),
        }
    }
}

/// Logical Plan for [Vacuum] operation.
///
/// [Vacuum]: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-vacuum
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Vacuum {
    /// A reference to the table being vacuumed
    pub table: TableReference,
    /// The retention threshold.
    pub retention_hours: Option<i32>,
    /// Return a list of up to 1000 files to be deleted.
    pub dry_run: bool,
    /// Schema for Vacuum's empty return table
    pub schema: DFSchemaRef,
}

impl PartialOrd for Vacuum {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl Vacuum {
    pub fn new(table: TableReference, retention_hours: Option<i32>, dry_run: bool) -> Self {
        Self {
            table,
            retention_hours,
            dry_run,
            schema: Arc::new(DFSchema::empty()),
        }
    }
}

/// Logical Plan for [DescribeHistory] operation.
///
/// [DescribeHistory]: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-describe-history
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DescribeHistory {
    /// A reference to the table
    pub table: TableReference,
    /// Schema for commit provenance information
    pub schema: DFSchemaRef,
}

impl PartialOrd for DescribeHistory {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl DescribeHistory {
    pub fn new(table: TableReference) -> Self {
        Self {
            table,
            // TODO: add proper schema
            // https://learn.microsoft.com/en-us/azure/databricks/delta/history#history-schema
            schema: Arc::new(DFSchema::empty()),
        }
    }
}

/// Logical Plan for DescribeDetails operation.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DescribeDetails {
    /// A reference to the table
    pub table: TableReference,
    /// Schema for commit provenance information
    pub schema: DFSchemaRef,
}

impl PartialOrd for DescribeDetails {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl DescribeDetails {
    pub fn new(table: TableReference) -> Self {
        Self {
            table,
            // TODO: add proper schema
            schema: Arc::new(DFSchema::empty()),
        }
    }
}

/// Logical Plan for DescribeFiles operation.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DescribeFiles {
    /// A reference to the table
    pub table: TableReference,
    /// Schema for commit provenance information
    pub schema: DFSchemaRef,
}

impl PartialOrd for DescribeFiles {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl DescribeFiles {
    pub fn new(table: TableReference) -> Self {
        Self {
            table,
            // TODO: add proper schema
            schema: Arc::new(DFSchema::empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        let stmt = DeltaStatement::Vacuum(Vacuum::new("table".into(), Some(1234), true));
        assert_eq!(
            format!("{}", stmt.display()),
            "Vacuum: table retention_hours=1234 dry_run=true"
        );

        let stmt = DeltaStatement::Vacuum(Vacuum::new("table".into(), None, true));
        assert_eq!(format!("{}", stmt.display()), "Vacuum: table dry_run=true")
    }
}
