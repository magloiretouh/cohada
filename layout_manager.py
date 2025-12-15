"""
Layout Manager for Grand Livre Reports
Handles loading and applying layout configurations for different business units
"""

import json
import os
import logging
from typing import Dict, List, Optional
import polars as pl

logger = logging.getLogger(__name__)


class LayoutManager:
    """
    Manages report layout configurations for different business units.
    Loads layout profiles from JSON and applies column exclusions and renaming.
    """

    def __init__(self, config_path: str = "report_layouts.json"):
        """
        Initialize the LayoutManager with a configuration file.

        Args:
            config_path: Path to the JSON configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """
        Load the layout configuration from JSON file.

        Returns:
            Dictionary containing layouts

        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Layout configuration file not found: {self.config_path}")

        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        # Validate config structure
        if "layouts" not in config:
            raise ValueError("Invalid layout configuration: missing 'layouts'")

        return config

    def get_all_layouts(self) -> Dict[str, Dict]:
        """
        Get all available layout configurations.

        Returns:
            Dictionary of all layouts
        """
        return self.config["layouts"]


# Convenience function for quick usage
def get_layout_manager() -> LayoutManager:
    """
    Get a singleton instance of LayoutManager.

    Returns:
        LayoutManager instance
    """
    if not hasattr(get_layout_manager, '_instance'):
        get_layout_manager._instance = LayoutManager()
    return get_layout_manager._instance
