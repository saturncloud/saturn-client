from typing import Any, Dict, List, Optional


def format_logs(pod_summary: Dict[str, Any], all_containers: bool = False) -> str:
    containers: List[Dict[str, Any]] = []
    init_container_summaries: List[Dict[str, Any]] = pod_summary.get("init_container_summaries", [])
    container_summaries: List[Dict[str, Any]] = pod_summary.get("container_summaries", [])
    if all_containers:
        containers.extend(init_container_summaries)
        containers.extend(container_summaries)
    else:
        # Find the main container
        for container in container_summaries:
            if container["name"] == "main":
                containers = [container]
                break
        if not containers:
            # If no main container, don't assume anything about which containers are important
            containers = container_summaries

        # If containers are waiting, init containers may still be running
        if all(c.get("status") == "waiting" for c in containers):
            containers = init_container_summaries

    container_logs = []
    for container in containers:
        previous_container = container.get("previous")
        if previous_container:
            container_logs.append(format_container_logs(previous_container, is_previous=True))
        container_logs.append(format_container_logs(container))

    logs = "\n\n".join(container_logs)
    if not logs:
        logs = f"Status: {pod_summary['status']}"
    return _section_header(f"Pod: {pod_summary['name']}", logs)


def is_live(pod_summary: Optional[Dict[str, Any]]) -> bool:
    """
    Return true if the pod summary represents a live pod for log formatting

    When a pod completes and its node shuts down, pods often come back without
    logs in state "completed".
    """
    if not pod_summary:
        return False
    if has_logs(pod_summary):
        return True
    return pod_summary["status"] not in {"completed", "stopping", "stopped"}


def has_logs(pod_summary: Dict[str, Any]) -> bool:
    containers = pod_summary.get("container_summaries", [])
    if any(c.get("logs") for c in containers):
        return True

    init_containers = pod_summary.get("init_container_summaries", [])
    return any(c.get("logs") for c in init_containers)


def format_container_logs(container_summary: Dict[str, Any], is_previous: bool = False) -> str:
    label = ""
    if is_previous:
        label = " (previous)"
    logs = container_summary["logs"]
    if not logs:
        logs = f"Status: {container_summary['status']}"
    logs = _section_header(f"Container: {container_summary['name']}{label}", logs, char="-")
    finished_at = container_summary.get("finished_at")
    if finished_at:
        finished_at = container_summary["finished_at"]
        exit_code = container_summary.get("exit_code")
        logs = _format_terminated(logs, finished_at, exit_code=exit_code)
    return logs


def format_historical_logs(pod_name: str, logs: str) -> str:
    logs = _section_header("Historical", logs, char="-")
    return _section_header(f"Pod: {pod_name}", logs)


def _format_terminated(
    logs: str, end_time: str, exit_code: Optional[int] = None, width: int = 100
) -> str:
    info = ""
    if exit_code is not None:
        info = f"{exit_code} "
    info += f"at {end_time}"
    footer = f" Terminated {info} "
    width -= len(footer)
    if width < 0:
        width = 0
    marker = "=" * int(width / 2)
    end_marker = marker
    if width % 2 == 1:
        end_marker += "="
    return f"{logs}\n{marker}{footer}{end_marker}"


def _section_header(label: str, content: str = "", char: str = "=", width: int = 100):
    return "\n".join(
        [
            label,
            (char * width),
            content,
        ]
    )
