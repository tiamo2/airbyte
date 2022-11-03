# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, validator  # noqa: F401
from connector_builder.generated.models.http_request import HttpRequest
from connector_builder.generated.models.http_response import HttpResponse


class StreamReadPages(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    StreamReadPages - a model defined in OpenAPI

        records: The records of this StreamReadPages.
        request: The request of this StreamReadPages.
        response: The response of this StreamReadPages.
    """

    records: List[object]
    request: HttpRequest
    response: HttpResponse

StreamReadPages.update_forward_refs()
