import warnings

try:
    from ibis.backends.databricks import Backend as DatabricksBackend

    def _no_op_post_connect(self, *args, **kwargs):
        # Intentionally do nothing to skip volume creation.
        # This effectively forces "read-only" mode regarding memtables
        pass

    DatabricksBackend._post_connect = _no_op_post_connect

except ImportError:
    pass
except Exception as e:
    warnings.warn(f"Mitos: Failed to patch Databricks backend: {e}")

from .cohort_expression import (
    CohortExpression,
    PrimaryCriteria,
    ResultLimit,
    ObservationFilter,
)
from .concept_set import ConceptSet, ConceptSetExpression, ConceptSetItem
from .criteria import (
    Criteria,
    CriteriaGroup,
    Concept,
    ConceptSetSelection,
    NumericRange,
    DateRange,
    TextFilter,
)
from .tables import ConditionOccurrence, ConditionEra, VisitOccurrence, DrugExposure
from .build_context import BuildContext, CohortBuildOptions, compile_codesets

__all__ = [
    "CohortExpression",
    "PrimaryCriteria",
    "ResultLimit",
    "ObservationFilter",
    "ConceptSet",
    "ConceptSetExpression",
    "ConceptSetItem",
    "Criteria",
    "CriteriaGroup",
    "Concept",
    "ConceptSetSelection",
    "NumericRange",
    "DateRange",
    "TextFilter",
    "ConditionOccurrence",
    "ConditionEra",
    "VisitOccurrence",
    "DrugExposure",
    "BuildContext",
    "CohortBuildOptions",
    "compile_codesets",
]
