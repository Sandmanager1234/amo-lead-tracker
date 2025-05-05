import os
from typing import Optional, List
from loguru import logger
from pydantic import BaseModel, Field, computed_field, model_validator, ConfigDict

from debug import TagCollector

# Ключевые слова для определения типа тега
TARGET_KEYWORDS = [
    "ss",
    "ent",
    "kids",
    "online",
    "ast",
    "ala",
    "two",
    "video",
    "WA",
    "FB",
    "FBONLINE",
    "WEB",
    "OLIMP",
    "PROG",
]

ZVONOBOT_KEYWORDS = ["звонобот", "zvonobot"]

PROCESSED_STATUSES = {
    os.getenv("astana_pipeline"): ["62970105", "62970109", "62970113"],
    os.getenv("almaty_pipeline"): ["63909053", "63909057", "63909061"],
    os.getenv("pipeline_online"): ["68601325", "68601329", "68601333"],
}

SUCCSES_PIPELINES = [
    os.getenv("pipeline_astana_success"),
    os.getenv("pipeline_almaty_success"),
]
ONLINE_STATUS_SUCCSES = os.getenv("status_online_success")


class Tag(BaseModel):
    id: int
    name: str

    @computed_field
    @property
    def type(self) -> Optional[str]:
        if (self.name is None) or self.name == "":
            return "other"

        name_lower = self.name.lower()
        # TagCollector().add_tag(self.name) - debug line
        if any(keyword.lower() in name_lower for keyword in TARGET_KEYWORDS):
            return "target"
        elif any(keyword.lower() in name_lower for keyword in ZVONOBOT_KEYWORDS):
            return "zvonobot"
        elif name_lower == "другой_город":
            return "other_city"
        return "other"


class Lead(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    status_id: int
    pipeline_id: int
    updated_at: int
    created_at: int
    closed_at: Optional[int]
    is_deleted: bool
    reject_reason: Optional[str] = None
    tags: List[Tag] = Field(default_factory=list)


    # "before" validators
    @model_validator(mode="before")
    @classmethod
    def extract_tags(cls, values: dict):
        logger.debug(f"Extracting tags from values: {values}")
        embedded = values.get("_embedded", {})
        values["tags"] = embedded.get("tags", [])
        if len(values["tags"]) == 0:
            values["tags"] = [Tag(id=0, name="")]
        return values
    
    @model_validator(mode="before")
    @classmethod
    def extract_reject_reason(cls, values: dict):
        fields = values.get("custom_fields_values", [])
        if not isinstance(fields, list):
            fields = []
        for field in fields:
            if field.get('field_name', None) == 'Причина отказа':
                values["reject_reason"] = field['values'][0]['value'] 
                break
        return values

    # Computed_fields

    @computed_field
    @property
    def is_success(self) -> bool:
        if str(self.pipeline_id) in SUCCSES_PIPELINES:
            return True
        elif str(self.status_id) == str(ONLINE_STATUS_SUCCSES):
            return True
        return False

    @computed_field
    @property
    def is_after_processing(self) -> bool:
        if (
            str(self.status_id) not in PROCESSED_STATUSES.get(str(self.pipeline_id), [])
            or self.is_success
        ):
            return True
        return False

    @computed_field
    @property
    def is_qualified(self) -> bool:
        if str(self.status_id) not in PROCESSED_STATUSES.get(
            str(self.pipeline_id), []
        ) and self.reject_reason in [
            "Передумали",
            "Не одобрили рассрочку",
            "не заполнено",
        ]:
            return True
        return False


class LeadsResponse(BaseModel):
    embedded: dict[str, List[Lead]] = Field(alias="_embedded")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @property
    def leads(self) -> List[Lead]:
        return self.embedded.get("leads", [])
