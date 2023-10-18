from typing import ClassVar

from pydantic import BaseModel


class DecsSchema(BaseModel):
    _decs_schema_name: ClassVar[str | None] = None

    class Config:
        arbitrary_types_allowed = True
