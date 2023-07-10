"""REST client handling, including branchioStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable
import io
import csv
import gzip
import sys
import datetime
import pendulum
import requests
from singer_sdk import metrics
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.helpers._util import utc_now


_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
csv.field_size_limit(sys.maxsize)


class branchioStream(RESTStream):
    """branchio stream class."""
    rest_method = "POST"
    primary_keys = ["id"]
    replication_key = "timestamp_iso"
    fields = (
        "id",
        "name",
        "timestamp",
        "timestamp_iso",
        "origin",
        "last_attributed_touch_type",
        "last_attributed_touch_timestamp",
        "last_attributed_touch_timestamp_iso",
        "last_attributed_touch_data_tilde_id",
        "last_attributed_touch_data_tilde_campaign",
        "last_attributed_touch_data_tilde_campaign_id",
        "last_attributed_touch_data_tilde_channel",
        "last_attributed_touch_data_tilde_feature",
        "last_attributed_touch_data_tilde_stage",
        "last_attributed_touch_data_tilde_tags",
        "last_attributed_touch_data_tilde_advertising_partner_name",
        "last_attributed_touch_data_tilde_secondary_publisher",
        "last_attributed_touch_data_tilde_creative_name",
        "last_attributed_touch_data_tilde_creative_id",
        "last_attributed_touch_data_tilde_ad_set_name",
        "last_attributed_touch_data_tilde_ad_set_id",
        "last_attributed_touch_data_tilde_ad_name",
        "last_attributed_touch_data_tilde_ad_id",
        "last_attributed_touch_data_tilde_branch_ad_format",
        "last_attributed_touch_data_tilde_technology_partner",
        "last_attributed_touch_data_tilde_banner_dimensions",
        "last_attributed_touch_data_tilde_placement",
        "last_attributed_touch_data_tilde_keyword_id",
        "last_attributed_touch_data_tilde_agency",
        "last_attributed_touch_data_tilde_optimization_model",
        "last_attributed_touch_data_tilde_secondary_ad_format",
        "last_attributed_touch_data_tilde_journey_name",
        "last_attributed_touch_data_tilde_journey_id",
        "last_attributed_touch_data_tilde_view_name",
        "last_attributed_touch_data_tilde_view_id",
        "last_attributed_touch_data_plus_current_feature",
        "last_attributed_touch_data_plus_via_features",
        "last_attributed_touch_data_dollar_3p",
        "last_attributed_touch_data_plus_web_format",
        "last_attributed_touch_data_custom_fields",
        "days_from_last_attributed_touch_to_event",
        "hours_from_last_attributed_touch_to_event",
        "minutes_from_last_attributed_touch_to_event",
        "seconds_from_last_attributed_touch_to_event",
        "last_cta_view_timestamp",
        "last_cta_view_timestamp_iso",
        "last_cta_view_data_tilde_id",
        "last_cta_view_data_tilde_campaign",
        "last_cta_view_data_tilde_campaign_id",
        "last_cta_view_data_tilde_channel",
        "last_cta_view_data_tilde_feature",
        "last_cta_view_data_tilde_stage",
        "last_cta_view_data_tilde_tags",
        "last_cta_view_data_tilde_advertising_partner_name",
        "last_cta_view_data_tilde_secondary_publisher",
        "last_cta_view_data_tilde_creative_name",
        "last_cta_view_data_tilde_creative_id",
        "last_cta_view_data_tilde_ad_set_name",
        "last_cta_view_data_tilde_ad_set_id",
        "last_cta_view_data_tilde_ad_name",
        "last_cta_view_data_tilde_ad_id",
        "last_cta_view_data_tilde_branch_ad_format",
        "last_cta_view_data_tilde_technology_partner",
        "last_cta_view_data_tilde_banner_dimensions",
        "last_cta_view_data_tilde_placement",
        "last_cta_view_data_tilde_keyword_id",
        "last_cta_view_data_tilde_agency",
        "last_cta_view_data_tilde_optimization_model",
        "last_cta_view_data_tilde_secondary_ad_format",
        "last_cta_view_data_plus_via_features",
        "last_cta_view_data_dollar_3p",
        "last_cta_view_data_plus_web_format",
        "last_cta_view_data_custom_fields",
        "deep_linked",
        "first_event_for_user",
        "user_data_os",
        "user_data_os_version",
        "user_data_model",
        "user_data_browser",
        "user_data_geo_country_code",
        "user_data_app_version",
        "user_data_sdk_version",
        "user_data_geo_dma_code",
        "user_data_environment",
        "user_data_platform",
        "user_data_aaid",
        "user_data_idfa",
        "user_data_idfv",
        "user_data_android_id",
        "user_data_limit_ad_tracking",
        "user_data_user_agent",
        "user_data_ip",
        "user_data_developer_identity",
        "user_data_language",
        "user_data_brand",
        "di_match_click_token",
        "event_data_revenue_in_usd",
        "event_data_exchange_rate",
        "event_data_transaction_id",
        "event_data_revenue",
        "event_data_currency",
        "event_data_shipping",
        "event_data_tax",
        "event_data_coupon",
        "event_data_affiliation",
        "event_data_search_query",
        "event_data_description",
        "custom_data",
        "last_attributed_touch_data_tilde_keyword",
        "user_data_cross_platform_id",
        "user_data_past_cross_platform_ids",
        "user_data_prob_cross_platform_ids",
        "store_install_begin_timestamp",
        "referrer_click_timestamp",
        "user_data_os_version_android",
        "user_data_geo_city_code",
        "user_data_geo_city_en",
        "user_data_http_referrer",
        "event_timestamp",
        "customer_event_alias",
        "last_attributed_touch_data_tilde_customer_campaign",
        "last_attributed_touch_data_tilde_campaign_type",
        "last_cta_view_data_tilde_campaign_type",
        "last_attributed_touch_data_tilde_agency_id",
        "last_attributed_touch_data_plus_touch_id",
        "last_cta_view_data_plus_touch_id",
        "user_data_installer_package_name",
        "user_data_cpu_type",
        "user_data_screen_width",
        "user_data_screen_height",
        "user_data_build",
        "user_data_internet_connection_type",
        "hash_version",
        "user_data_opted_in",
        "days_from_install_to_opt_in",
        "request_id",
        "last_attributed_touch_data_tilde_keyword_match_type",
        "last_attributed_touch_data_tilde_organic_search_url",
        "match_guaranteed",
        "user_data_opted_in_status",
        "last_attributed_touch_data_dollar_marketing_title",
        "user_data_oaid",
    )

    schema = th.PropertiesList(
        *[th.Property(i, th.StringType) for i in fields]
    ).to_dict()

    url_base = "https://api2.branch.io/v3/export"
    path = ""

    @property
    def requests_session(self) -> requests.Session:
        if not self._requests_session:
            self._requests_session = requests.Session()
            self._requests_session.stream = True
        return self._requests_session

    def prepare_request_payload(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ARG002
    ) -> dict | None:
        assert next_page_token is not None
        payload = {
            "branch_key": self.config.get("branch_key"),
            "branch_secret": self.config.get("branch_secret"),
            "export_date": next_page_token.strftime("%Y-%m-%d"),
        }
        return payload

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        start_date = (
            self.get_starting_replication_key_value(context)
            or self.config.get("start_date")
            or (utc_now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        )
        page_date = pendulum.parse(start_date)

        decorated_request = self.request_decorator(self._request)

        now = utc_now()

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while page_date.date() < now.date():
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=page_date,
                )
                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)
                yield from self.parse_response(resp)
                self.finalize_state_progress_markers()
                self._write_state_message()
                page_date += datetime.timedelta(days=1)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        stream_file_url = response.json()[self.name][0]
        stream_file_resp = requests.get(stream_file_url)
        stream_file_resp.raise_for_status()

        with gzip.open(io.BytesIO(stream_file_resp.content), "rt") as f:
            reader = csv.DictReader(f)
            yield from reader
