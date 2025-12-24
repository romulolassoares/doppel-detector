import toml
from pathlib import Path
from typing import Dict, Any, Optional


def load_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load configuration from a TOML file.

    Args:
        config_path: Path to the config file. If None, defaults to
                    'config/config.toml' relative to the project root.

    Returns:
        Dictionary containing the configuration. Returns empty dict if
        file is not found or if an error occurs.

    Example:
        >>> config = load_config()
        >>> db_config = config.get('database', {})
    """
    if config_path is None:
        # Default to config/config.toml relative to project root
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config" / "config.toml"

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return toml.load(f)
    except FileNotFoundError:
        # File not found is not critical, return empty config
        return {}
    except toml.TomlDecodeError as e:
        # Invalid TOML syntax - log but don't crash
        print(f"Error parsing TOML file '{config_path}': {e}")
        return {}
    except Exception as e:
        # Catch-all for other errors (permissions, etc.)
        print(f"Error loading config file '{config_path}': {e}")
        return {}


# Load default config at module level for backward compatibility
config = load_config()