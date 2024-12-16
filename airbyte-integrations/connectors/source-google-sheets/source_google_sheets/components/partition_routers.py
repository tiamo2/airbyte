#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
from typing import Any, Iterable, Mapping

from airbyte_cdk.sources.declarative.partition_routers.single_partition_router import SinglePartitionRouter
from airbyte_cdk.sources.types import StreamSlice


class RangePartitionRouter(SinglePartitionRouter):
    """
    Create ranges to request rows data to google sheets api.
    """

    parameters: Mapping[str, Any]

    def __init__(self, parameters: Mapping[str, Any]) -> None:
        super().__init__(parameters)
        self.parameters = parameters
        self.sheet_row_count = parameters.get("row_count", 0)
        self.batch_size = parameters.get("batch_size", 200)

    def stream_slices(self) -> Iterable[StreamSlice]:
        start_range = 2
        while start_range <= self.sheet_row_count:
            end_range = start_range + self.batch_size
            yield StreamSlice(partition={"start_range": start_range, "end_range": end_range}, cursor_slice={})
            start_range += self.batch_size + 1
