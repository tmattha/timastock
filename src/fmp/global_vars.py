def provide_api_key(provided_key: str) -> None:
    global _api_key
    _api_key = provided_key

def api_key() -> str:
    return _api_key