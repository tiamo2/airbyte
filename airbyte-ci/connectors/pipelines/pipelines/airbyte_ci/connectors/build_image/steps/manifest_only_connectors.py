#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any
from venv import logger

from packaging import version

from dagger import Container, Platform
from pipelines.airbyte_ci.connectors.build_image.steps import build_customization
from pipelines.airbyte_ci.connectors.build_image.steps.common import BuildConnectorImagesBase
from pipelines.airbyte_ci.connectors.context import ConnectorContext
from pipelines.consts import COMPONENTS_FILE_PATH, MANIFEST_FILE_PATH
from pipelines.models.steps import StepResult
from pydash.objects import get  # type: ignore


class BuildConnectorImages(BuildConnectorImagesBase):
    """
    A step to build a manifest only connector image.
    A spec command is run on the container to validate it was built successfully.
    """

    context: ConnectorContext
    PATH_TO_INTEGRATION_CODE = "/airbyte/integration_code"

    async def _build_connector(self, platform: Platform, *args: Any) -> Container:
        baseImage = get(self.context.connector.metadata, "connectorBuildOptions.baseImage")
        if not baseImage:
            raise ValueError("connectorBuildOptions.baseImage is required to build a manifest only connector.")
        
        self.logger.info("I AM MANIFEST_ONLY!!! RAWR!!!")
        self.logger.info(f"Path to integration_code: {self.PATH_TO_INTEGRATION_CODE}")

        return await self._build_from_base_image(platform, logger=self.logger)

    def _get_base_container(self, platform: Platform) -> Container:
        base_image_name = get(self.context.connector.metadata, "connectorBuildOptions.baseImage")
        self.logger.info(f"Building manifest connector from base image {base_image_name}")
        return self.dagger_client.container(platform=platform).from_(base_image_name)

    async def _build_from_base_image(self, platform: Platform, logger) -> Container:
        """Build the connector container using the base image defined in the metadata, in the connectorBuildOptions.baseImage field.

        Returns:
            Container: The connector container built from the base image.
        """
        self.logger.info(f"Building connector from base image in metadata for {platform}")

        

        if self._is_legacy_base_image():
            manifest_mount_path = f"source_declarative_manifest/{MANIFEST_FILE_PATH}"
            logger.info(f"Legacy base_image detected. Mounting manifest file at {manifest_mount_path}")
        
        else:
            manifest_mount_path = "/usr/local/lib/python3.10/site-packages/airbyte_cdk/manifest.yaml"
            logger.info(f"Non-legacy base_image detected. Mounting manifest file at {manifest_mount_path}")

        base_container = self._get_base_container(platform).with_file(
            manifest_mount_path,
            (await self.context.get_connector_dir(include=[MANIFEST_FILE_PATH])).file(MANIFEST_FILE_PATH),
        )

        # Mount components file if it exists
        components_file = self.context.connector.manifest_only_components_path
        if components_file.exists():
            base_container = base_container.with_file(
                f"source_declarative_manifest/{COMPONENTS_FILE_PATH}",
                (await self.context.get_connector_dir(include=[COMPONENTS_FILE_PATH])).file(COMPONENTS_FILE_PATH),
            )

        connector_container = build_customization.apply_airbyte_entrypoint(base_container, self.context.connector, logger)
        return connector_container
    
    def _is_legacy_base_image(self) -> bool:
        base_image_version = self.context.connector.base_image_version
        return version.parse(base_image_version) <= version.parse("6.5.2")


async def run_connector_build(context: ConnectorContext) -> StepResult:
    return await BuildConnectorImages(context).run()
