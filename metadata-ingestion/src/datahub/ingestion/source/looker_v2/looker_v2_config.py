"""
Looker V2 Source Configuration.

Unified source for Looker dashboards, explores, and LookML views.
"""

from __future__ import annotations

import dataclasses
import re
from typing import Any, ClassVar, Dict, List, Literal, Optional, Tuple, Union

import pydantic
from looker_sdk.sdk.api40.models import DBConnection
from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationError,
    HiddenFromDocs,
)
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPIConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

# ---------------------------------------------------------------------------
# Owned copies of naming pattern and connection config classes.
# Not imported from looker/ — standalone module.
# ---------------------------------------------------------------------------


class NamingPattern(ConfigModel):
    ALLOWED_VARS: ClassVar[List[str]] = []
    REQUIRE_AT_LEAST_ONE_VAR: ClassVar[bool] = True

    pattern: str

    @classmethod
    def __get_validators__(cls):
        yield cls.pydantic_accept_raw_pattern
        yield cls.validate
        yield cls.pydantic_validate_pattern

    @classmethod
    def pydantic_accept_raw_pattern(cls, v):
        if isinstance(v, (NamingPattern, dict)):
            return v
        assert isinstance(v, str), "pattern must be a string"
        return {"pattern": v}

    @model_validator(mode="before")
    @classmethod
    def pydantic_v2_accept_raw_pattern(cls, v):
        if isinstance(v, str):
            return {"pattern": v}
        return v

    @classmethod
    def pydantic_validate_pattern(cls, v):
        assert isinstance(v, NamingPattern)
        assert v.validate_pattern(cls.REQUIRE_AT_LEAST_ONE_VAR)
        return v

    @classmethod
    def allowed_docstring(cls) -> str:
        return f"Allowed variables are {cls.ALLOWED_VARS}"

    def validate_pattern(self, at_least_one: bool) -> bool:
        variables = re.findall("({[^}{]+})", self.pattern)
        variables = [v[1:-1] for v in variables]
        for v in variables:
            if v not in self.ALLOWED_VARS:
                raise ValueError(
                    f"Failed to find {v} in allowed_variables {self.ALLOWED_VARS}"
                )
        if at_least_one and len(variables) == 0:
            raise ValueError(
                f"Failed to find any variable assigned to pattern {self.pattern}. "
                f"Must have at least one. {self.allowed_docstring()}"
            )
        return True

    def replace_variables(self, values: Union[Dict[str, Optional[str]], object]) -> str:
        if not isinstance(values, dict):
            assert dataclasses.is_dataclass(values) and not isinstance(values, type)
            values = dataclasses.asdict(values)
        values = {k: v for k, v in values.items() if v is not None}
        return self.pattern.format(**values)


@dataclasses.dataclass
class NamingPatternMapping:
    platform: str
    env: str
    project: str
    model: str
    name: str


@dataclasses.dataclass
class ViewNamingPatternMapping(NamingPatternMapping):
    file_path: str
    folder_path: str


class LookerNamingPattern(NamingPattern):
    ALLOWED_VARS = [field.name for field in dataclasses.fields(NamingPatternMapping)]


class LookerViewNamingPattern(NamingPattern):
    ALLOWED_VARS = [
        field.name for field in dataclasses.fields(ViewNamingPatternMapping)
    ]


class LookerCommonConfig(EnvConfigMixin, PlatformInstanceConfigMixin):
    explore_naming_pattern: LookerNamingPattern = pydantic.Field(
        description=f"Pattern for providing dataset names to explores. {LookerNamingPattern.allowed_docstring()}",
        default=LookerNamingPattern(pattern="{model}.explore.{name}"),
    )
    explore_browse_pattern: LookerNamingPattern = pydantic.Field(
        description=f"Pattern for providing browse paths to explores. {LookerNamingPattern.allowed_docstring()}",
        default=LookerNamingPattern(pattern="/Explore/{model}"),
    )
    view_naming_pattern: LookerViewNamingPattern = Field(
        LookerViewNamingPattern(pattern="{project}.view.{name}"),
        description=f"Pattern for providing dataset names to views. {LookerViewNamingPattern.allowed_docstring()}",
    )
    view_browse_pattern: LookerViewNamingPattern = Field(
        LookerViewNamingPattern(pattern="/Develop/{project}/{folder_path}"),
        description=f"Pattern for providing browse paths to views. {LookerViewNamingPattern.allowed_docstring()}",
    )

    _deprecate_explore_browse_pattern = pydantic_field_deprecated(
        "explore_browse_pattern"
    )
    _deprecate_view_browse_pattern = pydantic_field_deprecated("view_browse_pattern")

    tag_measures_and_dimensions: bool = Field(
        True,
        description="When enabled, attaches tags to measures, dimensions and dimension groups. "
        "When disabled, adds this information to the description of the column.",
    )
    platform_name: HiddenFromDocs[str] = Field(
        "looker",
        description="Default platform name.",
    )
    extract_column_level_lineage: bool = Field(
        True,
        description="When enabled, extracts column-level lineage from Views and Explores",
    )


def _get_bigquery_definition(
    looker_connection: DBConnection,
) -> Tuple[str, Optional[str], Optional[str]]:
    platform = "bigquery"
    db = looker_connection.host
    schema = looker_connection.database
    return platform, db, schema


def _get_generic_definition(
    looker_connection: DBConnection, platform: Optional[str] = None
) -> Tuple[str, Optional[str], Optional[str]]:
    if platform is None:
        dialect_name = looker_connection.dialect_name
        assert dialect_name is not None
        platform = re.sub(r"[0-9]+", "", dialect_name.split("_")[0])
    assert platform is not None, (
        f"Failed to extract a valid platform from connection {looker_connection}"
    )
    db = looker_connection.database
    schema = looker_connection.schema
    return platform, db, schema


class LookerConnectionDefinition(ConfigModel):
    """Connection definition mapping a Looker connection to a DataHub platform."""

    platform: str
    default_db: str
    default_schema: Optional[str] = None
    platform_instance: Optional[str] = None
    platform_env: Optional[str] = Field(
        default=None,
        description="The environment that the platform is located in.",
    )

    @field_validator("platform_env", mode="after")
    @classmethod
    def platform_env_must_be_one_of(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return EnvConfigMixin.env_must_be_one_of(v)
        return v

    @field_validator("platform", "default_db", "default_schema", mode="after")
    @classmethod
    def lower_everything(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return v.lower()
        return v

    @classmethod
    def from_looker_connection(
        cls, looker_connection: DBConnection
    ) -> "LookerConnectionDefinition":
        """Create from a Looker SDK DBConnection object."""
        extractors: Dict[str, Any] = {
            "^bigquery": _get_bigquery_definition,
            ".*": _get_generic_definition,
        }

        if looker_connection.dialect_name is None:
            raise ConfigurationError(
                f"Unable to fetch a fully filled out connection for {looker_connection.name}. "
                "Please check your API permissions."
            )
        for extractor_pattern, extracting_function in extractors.items():
            if re.match(extractor_pattern, looker_connection.dialect_name):
                (platform, db, schema) = extracting_function(looker_connection)
                return cls(platform=platform, default_db=db, default_schema=schema)
        raise ConfigurationError(
            f"Could not find an appropriate platform for looker_connection: "
            f"{looker_connection.name} with dialect: {looker_connection.dialect_name}"
        )


class LookerV2GitInfo(ConfigModel):
    """Git repository information for cloning LookML projects."""

    repo: str = Field(description="Git repository URL")
    branch: str = Field(default="main", description="Git branch to checkout")
    deploy_key: Optional[pydantic.SecretStr] = Field(
        default=None, description="SSH deploy key for private repositories"
    )


class LookerV2Config(
    LookerAPIConfig,
    LookerCommonConfig,
    StatefulIngestionConfigBase,
):
    """
    Looker V2 source configuration.

    Extracts dashboards, looks, explores, and views from Looker.
    """

    # ==================== What to Extract ====================
    extract_dashboards: bool = Field(
        default=True,
        description="Extract dashboards and their charts.",
    )
    extract_looks: bool = Field(
        default=True,
        description="Extract standalone Looks.",
    )
    extract_explores: bool = Field(
        default=True,
        description="Extract LookML Explores as datasets.",
    )
    extract_views: bool = Field(
        default=False,
        description="Extract LookML Views. Requires base_folder or git_info.",
    )

    # ==================== LookML Project ====================
    base_folder: Optional[str] = Field(
        default=None,
        description="Path to LookML project folder. Alternative to git_info.",
    )
    git_info: Optional[LookerV2GitInfo] = Field(
        default=None,
        description="Git repository for LookML files. Alternative to base_folder.",
    )
    project_name: Optional[str] = Field(
        default=None,
        description="LookML project name. Auto-detected from manifest.lkml if not set.",
    )
    project_dependencies: Dict[str, Union[str, LookerV2GitInfo]] = Field(
        default_factory=dict,
        description="Map dependent project names to local paths or git repos.",
    )

    # ==================== Filtering ====================
    dashboard_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter dashboards by title.",
    )
    explore_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter explores by name.",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter views by name.",
    )
    model_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter LookML models by name.",
    )
    folder_path_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter dashboards by folder path (e.g., 'Shared/Sales').",
    )

    # ==================== Common Options ====================
    extract_owners: bool = Field(
        default=True,
        description="Extract ownership from Looker.",
    )
    extract_usage_history: bool = Field(
        default=True,
        description="Extract usage statistics for dashboards and looks.",
    )
    extract_usage_history_for_interval: str = Field(
        default="30 days",
        description="Interval to extract Looker usage history for. "
        "See https://docs.looker.com/reference/filter-expressions#date_and_time.",
    )
    skip_personal_folders: bool = Field(
        default=False,
        description="Skip dashboards and looks in personal folders.",
    )
    include_deleted: bool = Field(
        default=False,
        description="Include deleted dashboards/looks.",
    )
    strip_user_ids_from_email: bool = Field(
        default=False,
        description="When enabled, converts Looker user emails of the form name@domain.com "
        "to urn:li:corpuser:name when assigning ownership.",
    )
    include_platform_instance_in_urns: bool = Field(
        default=False,
        description="When enabled, platform instance will be added in dashboard and chart URNs. "
        "Required for migration compatibility if V1 had platform_instance set.",
    )
    external_base_url: Optional[str] = Field(
        default=None,
        description="Optional URL for constructing external URLs to Looker if base_url "
        "differs from the user-facing URL. Defaults to base_url.",
    )
    extract_embed_urls: bool = Field(
        default=True,
        description="Extract embed URLs for dashboards and explores.",
    )

    # ==================== Lineage ====================
    connection_to_platform_map: Dict[str, LookerConnectionDefinition] = Field(
        default_factory=dict,
        description="Map Looker connections to DataHub platforms for lineage. "
        "Connections not listed here will be auto-discovered from the Looker API.",
    )
    use_pdt_graph_api: bool = Field(
        default=False,
        description="Use Looker's PDT dependency graph API for derived table lineage. "
        "More reliable than SQL regex parsing for PDT-to-PDT dependencies. "
        "Falls back to SQL parsing when API data is unavailable. "
        "Requires the 'develop' permission on the Looker API client.",
    )
    emit_unreachable_views: bool = Field(
        default=False,
        description="Emit views not referenced by any explore.",
    )
    process_refinements: bool = Field(
        default=False,
        description="Process LookML view refinements.",
    )
    expand_refinement_lineage: bool = Field(
        default=False,
        description="When False (default), refinements are merged into the base view "
        "for V1 compatibility. When True, each refinement node becomes a separate "
        "Dataset entity with a lineage chain.",
    )

    # ==================== Performance ====================
    max_concurrent_requests: int = Field(
        default=10,
        description="Max concurrent API requests. Reduce if hitting rate limits.",
    )

    # ==================== Template Variables ====================
    liquid_variables: Dict[str, Any] = Field(
        default_factory=dict,
        description="Variables for Liquid template substitution in LookML SQL. "
        "These resolve {{ variable }} syntax in sql_table_name and derived_table.sql.",
    )
    lookml_constants: Dict[str, str] = Field(
        default_factory=dict,
        description="LookML constants for @{constant_name} substitution. "
        "If a constant is defined in manifest.lkml, the manifest value takes precedence.",
    )
    looker_environment: Literal["prod", "dev"] = Field(
        default="prod",
        description="Looker environment (prod or dev). Controls evaluation of "
        "'-- if prod --' / '-- if dev --' comment directives in LookML SQL.",
    )

    # ==================== Stateful Ingestion ====================
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Enable stateful ingestion for stale entity removal.",
    )

    # ==================== Remove Deprecated Inherited Fields ====================
    _remove_explore_browse_pattern = pydantic_removed_field("explore_browse_pattern")
    _remove_view_browse_pattern = pydantic_removed_field("view_browse_pattern")

    # ==================== Validators ====================
    @model_validator(mode="before")
    @classmethod
    def external_url_defaults_to_base_url(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Default external_base_url to base_url if not provided."""
        if isinstance(values, dict):
            if "external_base_url" not in values or values["external_base_url"] is None:
                values["external_base_url"] = values.get("base_url")
        return values

    @model_validator(mode="after")
    def validate_views_require_lookml_access(self) -> "LookerV2Config":
        """Validate that base_folder or git_info is provided for views."""
        if self.extract_views and not self.base_folder and not self.git_info:
            raise ValueError(
                "Either 'base_folder' or 'git_info' must be provided when 'extract_views' is True"
            )
        return self

    @field_validator("project_dependencies", mode="before")
    @classmethod
    def parse_project_dependencies(
        cls, v: Dict[str, Any]
    ) -> Dict[str, Union[str, LookerV2GitInfo]]:
        """Parse project dependencies, converting dicts to GitInfo."""
        if not v:
            return {}
        result: Dict[str, Union[str, LookerV2GitInfo]] = {}
        for name, value in v.items():
            if isinstance(value, str):
                result[name] = value
            elif isinstance(value, dict):
                result[name] = LookerV2GitInfo(**value)
            elif isinstance(value, LookerV2GitInfo):
                result[name] = value
            else:
                raise ValueError(f"Invalid project dependency for '{name}': {value}")
        return result
