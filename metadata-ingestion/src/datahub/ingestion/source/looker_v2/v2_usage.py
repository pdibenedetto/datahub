"""
Usage statistics module for Looker V2 source.

Re-exports the usage stat classes and generators from looker_usage.
The stat generators are complex infrastructure (600+ lines, LookerUserRegistry coupling)
that would require copying all of looker_common.py if isolated. They are stable
utilities with no business logic change needed, so re-exporting is the right trade-off.
"""

# Re-export usage stat classes for use within looker_v2_source.
# These are the only looker/ imports permitted in looker_v2/ beyond looker_lib_wrapper.
from datahub.ingestion.source.looker.looker_usage import (  # noqa: F401
    LookerChartForUsage,
    LookerDashboardForUsage,
    StatGeneratorConfig,
    create_chart_stat_generator,
    create_dashboard_stat_generator,
)
