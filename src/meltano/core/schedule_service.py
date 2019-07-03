from collections import namedtuple
from typing import Optional
from datetime import datetime, date

from .plugin.settings_service import SettingsService
from .project import Project
from .plugin import PluginType, PluginRef
from .db import project_engine
from .utils import nest, iso8601_datetime


class ScheduleAlreadyExistsError(Exception):
    """Occurs when a schedule already exists."""

    def __init__(self, schedule):
        self.schedule = schedule


Schedule = namedtuple(
    "Schedule",
    ("name", "extractor", "loader", "transform", "interval", "start_date", "env"),
)


class ScheduleService:
    def __init__(
        self,
        session,
        project: Project,
        plugin_settings_service: SettingsService = None,
    ):
        self.project = project
        self.settings_service = plugin_settings_service or SettingsService(
            session, project
        )
        self._session = session

    def add(
        self,
        name,
        extractor: str,
        loader: str,
        transform: str,
        interval: str,
        start_date: Optional[datetime] = None,
        **env
    ):
        start_date = start_date or self.default_start_date(extractor)
        schedule = Schedule(
            name, extractor, loader, transform, interval, start_date, env=env
        )

        return self.add_schedule(schedule)

    def default_start_date(self, extractor: str) -> datetime:
        """
        Returns the `start_date` of the extractor, or now.
        """
        extractor_ref = PluginRef(PluginType.EXTRACTORS, extractor)
        start_date = self.settings_service.get_value(extractor_ref, "start_date")

        # TODO: this coercion should be handled by the `kind` attribute
        # on the actual setting
        if isinstance(start_date, datetime):
            return start_date

        if isinstance(start_date, date):
            return coerce_datetime(start_date)

        return iso8601_datetime(start_date) or datetime.utcnow()

    def add_schedule(self, schedule: Schedule):
        # guard if it already exists
        if any(map(lambda s: s.name == schedule.name, self.schedules())):
            raise ScheduleAlreadyExistsError(schedule)

        with self.project.meltano_update() as meltano:
            # find the orchestrator plugin config
            schedules = nest(meltano, "schedules", value=[])
            schedules.append(dict(schedule._asdict()))

        return schedule

    def schedules(self):
        return (
            self.yaml_schedule(schedule_def)
            for schedule_def in self.project.meltano.get("schedules", [])
        )

    @classmethod
    def yaml_schedule(cls, schedule_definition: dict) -> Schedule:
        return Schedule(**schedule_definition)
