from api_configs import API_CONFIGS


def fetch_from_py_config(config_name: str, auth: Union[Tuple[str, str], requests.auth.AuthBase],
                         headers: Optional[Dict[str, str]] = None) -> Optional[Dict[Any, Any]]:
    """Fetch data using Python configuration."""
    if config_name not in API_CONFIGS:
        print(f"Configuration '{config_name}' not found")
        return None

    config = API_CONFIGS[config_name]
    return fetch_api_data(config['url'], auth,
                          params=config['params'], headers=headers)





