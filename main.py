import os
import httpx
import logging
from typing import Any, List, Optional
from urllib.parse import quote
from datetime import datetime
import base64
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp import FastMCP
from fastmcp.server.context import Context
from fastmcp.server.dependencies import get_http_request
# ============================================================
# LOG DIRECTORY SETUP
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "logs"))
os.makedirs(LOG_DIR, exist_ok=True)

SERVER_LOG_FILE = os.path.join(LOG_DIR, "server.log")
USAGE_LOG_FILE = os.path.join(LOG_DIR, "usage.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(SERVER_LOG_FILE),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("gitlab-mcp")


# ============================================================
# MCP & GITLAB CONFIG
# ============================================================

GITLAB_BASE_URL = os.getenv("GITLAB_BASE_URL", "https://gitlab.com").rstrip("/")
API_URL = f"{GITLAB_BASE_URL}/api/v4"




# --- Middleware Implementation ---

class GitLabAuthMiddleware(Middleware):
    async def on_request(self, context: MiddlewareContext, call_next):
        """ Runs before every tool or resource request """

        request = get_http_request()
        if not request:
            # If there's no HTTP request, we might be in STDIO mode
            return await call_next(context)

        auth_header = request.headers.get("authorization")
        if not auth_header or not auth_header.lower().startswith("bearer "):
            raise RuntimeError("Missing or invalid Authorization header")

        token = auth_header.split(" ", 1)[1]

        # 2. Validate Token and Get Username & Groups
        async with httpx.AsyncClient(timeout=10.0) as client:
            headers = {"PRIVATE-TOKEN": token}

            # Fetch user profile to get username
            user_res = await client.get(f"{API_URL}/user", headers=headers)
            if user_res.status_code != 200:
                raise RuntimeError("Invalid GitLab Token")

            user_data = user_res.json()
            print(user_data)
            username = user_data.get("username")
            print(username)





        # 4. Inject username into context so tools can use it if needed
        # We can store it in the context's state for the duration of this request
        context.fastmcp_context.set_state("username", username)


        # Proceed to the actual tool execution
        return await call_next(context)

mcp = FastMCP("gitlab-mcp")
mcp.add_middleware(GitLabAuthMiddleware())
# ============================================================
# USAGE LOGGING
# ============================================================

async def resolve_branch(token: str, project_id: int, branch: Optional[str]) -> str:
    """Internal helper to get the provided branch or fall back to project default."""
    if branch:
        return branch
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{API_URL}/projects/{project_id}", headers={"PRIVATE-TOKEN": token})
        res.raise_for_status()
        return res.json().get("default_branch", "main")

def log_usage(kind: str, name: str, params: dict,username):
    """
    Writes tool/resource usage to logs/usage.log

    Format:
    <ISO_TIMESTAMP> | <kind>=<name> | params=<dict>
    """
    timestamp = datetime.utcnow().isoformat()
    with open(USAGE_LOG_FILE, "a") as f:
        f.write(f"{timestamp} | {kind}={name} | params={params}\n | {username}\n")

# ============================================================
# CORE UTILITIES
# ============================================================

def extract_gitlab_token(ctx: Context) -> str:
    """
    Extracts GitLab Personal Access Token from HTTP headers.

    Expected header:
    Authorization: Bearer <gitlab_pat>

    This token is then passed to GitLab using PRIVATE-TOKEN header.
    """
    request = getattr(ctx, "request", None)
    if not request and hasattr(ctx, "request_context"):
        request = ctx.request_context.request

    if not request:
        raise RuntimeError("No HTTP request context found. Use HTTP transport.")

    auth = request.headers.get("authorization")
    if not auth or not auth.lower().startswith("bearer "):
        raise RuntimeError("Missing Authorization: Bearer <token> header")

    return auth.split(" ", 1)[1]


async def gitlab_request(
    token: str,
    method: str,
    endpoint: str,
    params: dict = None,
    json_data: dict = None,
) -> Any:
    """
    Generic async GitLab API requester.

    - Handles authentication
    - Raises HTTP errors transparently
    - Returns parsed JSON
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.request(
            method,
            f"{API_URL}/{endpoint.lstrip('/')}",
            headers={"PRIVATE-TOKEN": token},
            params=params,
            json=json_data,
        )
        response.raise_for_status()
        return response.json()


def resolve_project_identifier(
    project_id: Optional[int],
    project_name: Optional[str],
) -> str:
    """
    Resolves GitLab project identifier.

    Input:
    - project_id (numeric GitLab project ID)
    OR
    - project_name (full path_with_namespace, e.g. group/subgroup/project)

    Output:
    - Identifier suitable for GitLab API paths
    """
    if project_id:
        return str(project_id)
    if project_name:
        return quote(project_name, safe="")
    raise RuntimeError("Either project_id or project_name must be provided")

# ============================================================
# HEALTH & DISCOVERY TOOLS
# ============================================================

@mcp.tool()
async def health_check() -> dict:
    """
    Checks whether the GitLab MCP server is running and configured.

    Output:
    {
      status: "ok",
      service: "gitlab-mcp",
      configured_base_url: "<gitlab_url>"
    }
    """
    log_usage("tool", "health_check", {})
    return {
        "status": "ok",
        "service": "gitlab-mcp",
        "configured_base_url": GITLAB_BASE_URL,
    }


@mcp.tool()
async def list_projects(ctx: Context) -> List[dict]:
    """
    Lists all GitLab projects the authenticated user has access to.

    Output schema (per project):
    {
      id: number,
      name: string,
      path_with_namespace: string,
      visibility: string,
      default_branch: string | null
    }

    Use this tool when:
    - The user wants to discover projects
    - You need project_id or project_name for other tools
    """
    username = ctx.get_state("username")
    log_usage("tool", "list_projects", {},username)
    token = extract_gitlab_token(ctx)

    projects = await gitlab_request(
        token,
        "GET",
        "projects",
        params={"membership": True, "per_page": 100},
    )

    return [
        {
            "id": p["id"],
            "name": p["name"],
            "path_with_namespace": p["path_with_namespace"],
            "visibility": p["visibility"],
            "default_branch": p.get("default_branch"),
        }
        for p in projects
    ]


@mcp.tool()
async def create_project(
    ctx: Context,
    name: str,
    visibility: str = "private",
    namespace_id: Optional[int] = None,
) -> dict:
    """
    Creates a new GitLab project.

    Input:
    - name: project name
    - visibility: private | internal | public
    - namespace_id (optional): target namespace/group

    Output:
    {
      id: number,
      name: string,
      web_url: string,
      status: "created"
    }
    """
    log_usage(
        "tool",
        "create_project",
        {"name": name, "visibility": visibility, "namespace_id": namespace_id},
    )
    token = extract_gitlab_token(ctx)

    payload = {"name": name, "visibility": visibility}
    if namespace_id:
        payload["namespace_id"] = namespace_id

    project = await gitlab_request(token, "POST", "projects", json_data=payload)

    return {
        "id": project["id"],
        "name": project["name_with_namespace"],
        "web_url": project["web_url"],
        "status": "created",
    }

# ============================================================
# BRANCH TOOLS
# ============================================================

@mcp.tool()
async def list_branches(
    ctx: Context,
    project_id: Optional[int] = None,
    project_name: Optional[str] = None,
) -> List[dict]:
    """
    Lists all branches of a GitLab project.

    Input:
    - project_id OR project_name

    Output schema:
    {
      name: string,
      protected: boolean
    }
    """
    log_usage(
        "tool",
        "list_branches",
        {"project_id": project_id, "project_name": project_name},
    )
    token = extract_gitlab_token(ctx)
    project = resolve_project_identifier(project_id, project_name)

    branches = await gitlab_request(
        token,
        "GET",
        f"projects/{project}/repository/branches",
        params={"per_page": 100},
    )

    return [{"name": b["name"], "protected": b["protected"]} for b in branches]


@mcp.tool()
async def create_branch(
    ctx: Context,
    branch: str,
    ref: str,
    project_id: Optional[int] = None,
    project_name: Optional[str] = None,
) -> dict:
    """
    Creates a new branch in a GitLab repository.

    Input:
    - branch: name of new branch
    - ref: source branch or commit SHA
    - project_id OR project_name

    Output:
    {
      branch: string,
      status: "created"
    }
    """
    log_usage(
        "tool",
        "create_branch",
        {
            "branch": branch,
            "ref": ref,
            "project_id": project_id,
            "project_name": project_name,
        },
    )
    token = extract_gitlab_token(ctx)
    project = resolve_project_identifier(project_id, project_name)

    data = await gitlab_request(
        token,
        "POST",
        f"projects/{project}/repository/branches",
        params={"branch": branch, "ref": ref},
    )

    return {"branch": data["name"], "status": "created"}

# ============================================================
# MERGE REQUEST TOOLS
# ============================================================

@mcp.tool()
async def list_merge_requests(
    ctx: Context,
    project_id: Optional[int] = None,
    project_name: Optional[str] = None,
    state: str = "opened",
) -> List[dict]:
    """
    Lists merge requests of a project.

    Input:
    - project_id OR project_name
    - state: opened | closed | merged | all

    Output schema:
    {
      iid: number,
      title: string,
      state: string,
      source_branch: string,
      target_branch: string
    }
    """
    log_usage(
        "tool",
        "list_merge_requests",
        {"project_id": project_id, "project_name": project_name, "state": state},
    )
    token = extract_gitlab_token(ctx)
    project = resolve_project_identifier(project_id, project_name)

    mrs = await gitlab_request(
        token,
        "GET",
        f"projects/{project}/merge_requests",
        params={"state": state, "per_page": 50},
    )

    return [
        {
            "iid": mr["iid"],
            "title": mr["title"],
            "state": mr["state"],
            "source_branch": mr["source_branch"],
            "target_branch": mr["target_branch"],
        }
        for mr in mrs
    ]


@mcp.tool()
async def create_merge_request(
    ctx: Context,
    source_branch: str,
    target_branch: str,
    title: str,
    project_id: Optional[int] = None,
    project_name: Optional[str] = None,
    description: Optional[str] = None,
    remove_source_branch: bool = True,
) -> dict:
    """
    Creates a new merge request.

    Input:
    - source_branch
    - target_branch
    - title
    - description (optional)
    - remove_source_branch (default true)
    - project_id OR project_name

    Output:
    {
      iid: number,
      web_url: string,
      status: "created"
    }
    """
    log_usage(
        "tool",
        "create_merge_request",
        {
            "source_branch": source_branch,
            "target_branch": target_branch,
            "title": title,
            "project_id": project_id,
            "project_name": project_name,
        },
    )
    token = extract_gitlab_token(ctx)
    project = resolve_project_identifier(project_id, project_name)

    mr = await gitlab_request(
        token,
        "POST",
        f"projects/{project}/merge_requests",
        json_data={
            "source_branch": source_branch,
            "target_branch": target_branch,
            "title": title,
            "description": description,
            "remove_source_branch": remove_source_branch,
        },
    )

    return {"iid": mr["iid"], "web_url": mr["web_url"], "status": "created"}

# ============================================================
# COMMITS & FILE TOOLS
# ============================================================

@mcp.tool()
async def list_commits(
    ctx: Context,
    project_id: Optional[int] = None,
    project_name: Optional[str] = None,
    ref_name: Optional[str] = None,
) -> List[dict]:
    """
    Lists commits in a repository.

    Input:
    - project_id OR project_name
    - ref_name (optional branch/tag)

    Output schema:
    {
      id: string,
      short_id: string,
      title: string,
      author: string,
      created_at: string
    }
    """
    log_usage(
        "tool",
        "list_commits",
        {"project_id": project_id, "project_name": project_name, "ref_name": ref_name},
    )
    token = extract_gitlab_token(ctx)
    project = resolve_project_identifier(project_id, project_name)

    params = {"per_page": 50}
    if ref_name:
        params["ref_name"] = ref_name

    commits = await gitlab_request(
        token, "GET", f"projects/{project}/repository/commits", params=params
    )

    return [
        {
            "id": c["id"],
            "short_id": c["short_id"],
            "title": c["title"],
            "author": c["author_name"],
            "created_at": c["created_at"],
        }
        for c in commits
    ]


@mcp.tool()
async def get_file(
    ctx: Context,
    file_path: str,
    ref: str,
    project_id: Optional[int] = None,
    project_name: Optional[str] = None,
) -> dict:
    """
    Retrieves file content from a GitLab repository.

    Input:
    - file_path: path inside repository (e.g. src/app.py)
    - ref: branch, tag, or commit SHA
    - project_id OR project_name

    Output:
    {
      file_path: string,
      ref: string,
      content: string (decoded text)
    }
    """
    log_usage(
        "tool",
        "get_file",
        {
            "file_path": file_path,
            "ref": ref,
            "project_id": project_id,
            "project_name": project_name,
        },
    )
    token = extract_gitlab_token(ctx)
    project = resolve_project_identifier(project_id, project_name)

    encoded_path = quote(file_path, safe="")
    file_data = await gitlab_request(
        token,
        "GET",
        f"projects/{project}/repository/files/{encoded_path}",
        params={"ref": ref},
    )

    content = base64.b64decode(file_data["content"]).decode("utf-8", errors="ignore")

    return {"file_path": file_path, "ref": ref, "content": content}

# ============================================================
# RESOURCES
# ============================================================

@mcp.resource("gitlab://system/version")
async def gitlab_version(ctx: Context) -> str:
    """
    Returns GitLab system version and revision.
    """
    log_usage("resource", "gitlab_version", {})
    token = extract_gitlab_token(ctx)
    data = await gitlab_request(token, "GET", "version")
    return f"GitLab Version: {data['version']}\nRevision: {data['revision']}"


@mcp.resource("project://{project_id}/details")
async def project_details(project_id: str, ctx: Context) -> str:
    """
    Returns detailed metadata about a GitLab project.
    """
    log_usage("resource", "project_details", {"project_id": project_id})
    token = extract_gitlab_token(ctx)
    project = await gitlab_request(token, "GET", f"projects/{project_id}")

    return "\n".join(
        [
            f"Name: {project['name_with_namespace']}",
            f"ID: {project['id']}",
            f"Description: {project.get('description') or 'No description'}",
            f"Visibility: {project['visibility']}",
            f"Default Branch: {project.get('default_branch', 'N/A')}",
            f"Last Activity: {project['last_activity_at']}",
        ]
    )


# --- Git Simulation Tools ---

@mcp.tool()
async def git_status(ctx: Context, project_id: int, branch: Optional[str] = None) -> dict:
    """
    Simulates 'git status' by retrieving branch metadata and the latest commit info.
    Use this to see if a branch exists, its protection status, and the latest SHA.
    - project_id: Numeric ID of the project.
    - branch: Optional. The branch to check. Defaults to the project's default branch.
    """
    token = ctx.get_state("token")
    target_branch = await resolve_branch(token, project_id, branch)
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{API_URL}/projects/{project_id}/repository/branches/{target_branch}", headers={"PRIVATE-TOKEN": token})
        return res.json()


@mcp.tool()
async def git_commit(
        ctx: Context,
        project_id: int,
        message: str,
        changes: List[dict],
        branch: Optional[str] = None
) -> dict:
    """
    Simulates 'git add .' and 'git commit' by committing multiple files at once.
    - project_id: Numeric ID of the project.
    - message: The commit message.
    - changes: A list of dictionaries, each containing:
        - 'file_path': The path to the file.
        - 'content': The new content of the file.
        - 'action': (Optional) 'create', 'update', or 'delete'. Defaults to 'update'.
    - branch: Optional. Target branch. Defaults to project's default branch.
    """
    token = ctx.get_state("token")
    target_branch = await resolve_branch(token, project_id, branch)

    # Format the actions for the GitLab Commits API
    actions = []
    for change in changes:
        actions.append({
            "action": change.get("action", "update"),
            "file_path": change["file_path"],
            "content": change.get("content", ""),
            "encoding": "text"
        })

    payload = {
        "branch": target_branch,
        "commit_message": message,
        "actions": actions
    }

    async with httpx.AsyncClient() as client:
        url = f"{API_URL}/projects/{project_id}/repository/commits"
        res = await client.post(url, headers={"PRIVATE-TOKEN": token}, json=payload)

        # If any file in the 'update' list doesn't exist, you might get a 400.
        # Production tip: In a real 'git add .' scenario, you'd check file existence first.
        res.raise_for_status()
        return res.json()

@mcp.tool()
async def git_checkout(ctx: Context, project_id: int, branch_name: str, start_point: Optional[str] = None) -> dict:
    """
    Simulates 'git checkout -b'. Creates a new branch from a starting point.
    - project_id: Numeric ID of the project.
    - branch_name: The name of the new branch to create.
    - start_point: Optional. The source branch/SHA to branch from. Defaults to project's default branch.
    """
    token = ctx.get_state("token")
    ref = await resolve_branch(token, project_id, start_point)
    async with httpx.AsyncClient() as client:
        res = await client.post(
            f"{API_URL}/projects/{project_id}/repository/branches",
            headers={"PRIVATE-TOKEN": token},
            params={"branch": branch_name, "ref": ref}
        )
        res.raise_for_status()
        return res.json()


# --- DevOps & Git Tools with Usage Logging ---

@mcp.tool()
async def get_merge_diff(ctx: Context, project_id: int, merge_request_iid: int) -> List[dict]:
    """
    Retrieves changes/diffs for a specific merge request.
    - merge_request_iid: The internal IID (e.g., 5) of the MR.
    """
    username = ctx.get_state("username")
    log_usage("tool", "get_merge_diff", {"pid": project_id, "iid": merge_request_iid}, username)

    token = ctx.get_state("token")
    async with httpx.AsyncClient() as client:
        url = f"{API_URL}/projects/{project_id}/merge_requests/{merge_request_iid}/changes"
        res = await client.get(url, headers={"PRIVATE-TOKEN": token})
        res.raise_for_status()
        return res.json().get("changes", [])


@mcp.tool()
async def list_repository_trees(ctx: Context, project_id: int, path: str = None, recursive: bool = False) -> List[dict]:
    """
    Lists files and directories. Use 'recursive=True' to see all nested files.
    """
    username = ctx.get_state("username")
    log_usage("tool", "list_repository_trees", {"pid": project_id, "path": path}, username)

    token = ctx.get_state("token")
    async with httpx.AsyncClient() as client:
        params = {"path": path, "recursive": recursive, "per_page": 100}
        res = await client.get(f"{API_URL}/projects/{project_id}/repository/tree", headers={"PRIVATE-TOKEN": token},
                               params=params)
        res.raise_for_status()
        return res.json()


@mcp.tool()
async def add_mr_comment(ctx: Context, project_id: int, merge_request_iid: int, body: str) -> dict:
    """
    Adds a Markdown comment to a merge request discussion.
    """
    username = ctx.get_state("username")
    log_usage("tool", "add_mr_comment", {"pid": project_id, "iid": merge_request_iid}, username)

    token = ctx.get_state("token")
    async with httpx.AsyncClient() as client:
        url = f"{API_URL}/projects/{project_id}/merge_requests/{merge_request_iid}/notes"
        res = await client.post(url, headers={"PRIVATE-TOKEN": token}, json={"body": body})
        res.raise_for_status()
        return {"status": "success", "note_id": res.json()["id"]}


@mcp.tool()
async def trigger_pipeline(ctx: Context, project_id: int, ref: str = "main") -> dict:
    """
    Triggers a CI/CD pipeline for a specific branch or tag.
    """
    username = ctx.get_state("username")
    log_usage("tool", "trigger_pipeline", {"pid": project_id, "ref": ref}, username)

    token = ctx.get_state("token")
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{API_URL}/projects/{project_id}/pipeline", headers={"PRIVATE-TOKEN": token},
                                params={"ref": ref})
        res.raise_for_status()
        return res.json()


@mcp.tool()
async def get_pipeline_status(ctx: Context, project_id: int, pipeline_id: int) -> dict:
    """
    Checks the current status (running, success, failed) of a pipeline.
    """
    username = ctx.get_state("username")
    log_usage("tool", "get_pipeline_status", {"pid": project_id, "pipeline_id": pipeline_id}, username)

    token = ctx.get_state("token")
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{API_URL}/projects/{project_id}/pipelines/{pipeline_id}",
                               headers={"PRIVATE-TOKEN": token})
        res.raise_for_status()
        return res.json()


@mcp.tool()
async def get_job_logs(ctx: Context, project_id: int, job_id: int) -> str:
    """
    Retrieves raw log output from a CI/CD job for troubleshooting.
    """
    username = ctx.get_state("username")
    log_usage("tool", "get_job_logs", {"pid": project_id, "job_id": job_id}, username)

    token = ctx.get_state("token")
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{API_URL}/projects/{project_id}/jobs/{job_id}/trace", headers={"PRIVATE-TOKEN": token})
        res.raise_for_status()
        return res.text


@mcp.tool()
async def assign_issue(ctx: Context, project_id: int, issue_iid: int, username_to_assign: str) -> dict:
    """
    Assigns an issue to a user by their GitLab username.
    """
    username = ctx.get_state("username")
    log_usage("tool", "assign_issue", {"pid": project_id, "issue": issue_iid, "target": username_to_assign}, username)

    token = ctx.get_state("token")
    async with httpx.AsyncClient() as client:
        user_res = await client.get(f"{API_URL}/users", headers={"PRIVATE-TOKEN": token},
                                    params={"username": username_to_assign})
        users = user_res.json()
        if not users: return {"error": "User not found"}

        res = await client.put(f"{API_URL}/projects/{project_id}/issues/{issue_iid}", headers={"PRIVATE-TOKEN": token},
                               json={"assignee_ids": [users[0]["id"]]})
        res.raise_for_status()
        return {"status": "success", "assigned_to": username_to_assign}


@mcp.tool()
async def list_project_members(ctx: Context, project_id: int) -> List[dict]:
    """
    Lists all users who have access to the project.
    Use this to find valid usernames for the 'assign_issue' tool.
    """
    username = ctx.get_state("username")
    log_usage("tool", "list_project_members", {"pid": project_id}, username)

    token = ctx.get_state("token")
    async with httpx.AsyncClient() as client:
        url = f"{API_URL}/projects/{project_id}/members"
        res = await client.get(url, headers={"PRIVATE-TOKEN": token})
        res.raise_for_status()

        members = res.json()
        return [{"id": m["id"], "username": m["username"], "name": m["name"], "access_level": m["access_level"]} for m
                in members]


from typing import Optional, List, Literal, Union


@mcp.tool()
async def advanced_search(
        ctx: Context,
        query: str,
        scope: Literal[
            "projects", "issues", "merge_requests", "milestones",
            "wiki_blobs", "commits", "blobs", "users", "notes", "snippet_titles"
        ] = "projects",
        project_id: Optional[Union[int, str]] = None,
        group_id: Optional[Union[int, str]] = None,
        search_type: Literal["basic", "advanced", "zoekt"] = "basic",
        regex: bool = False,
        state: Optional[Literal["opened", "closed"]] = None,
        confidential: bool = False,
        order_by: Literal["created_at"] = "created_at",
        sort: Literal["asc", "desc"] = "desc",
        fields: Optional[List[str]] = None
) -> List[dict]:
    """
    Performs a deep search across GitLab projects, code, issues, and more.
    Supports global, group-level, and project-level scopes with advanced filters.

    Args:
        query: The search term or regex pattern.
        scope: The type of data to search for. Default is 'projects'.
        project_id: (Optional) Limit search to a specific project.
        group_id: (Optional) Limit search to a specific group.
        search_type: 'zoekt' for exact code search (if enabled), 'advanced' for elasticsearch, 'basic' for standard.
        regex: Set to True to use regular expressions (requires 'zoekt' or 'advanced' search).
        state: Filter issues/merge_requests by 'opened' or 'closed'.
        confidential: Filter by confidentiality (only applies to 'issues' scope).
        order_by: Column to sort results by. Currently only 'created_at' is supported.
        sort: Sort direction ('asc' or 'desc').
        fields: (Optional) Fields to search in (e.g., ['title']). Applies only to issues/MRs.
    """
    username = ctx.get_state("username")
    log_usage("tool", "advanced_search", {"query": query, "scope": scope}, username)

    token = ctx.get_state("token")

    # Construct the correct endpoint URL
    if project_id:
        endpoint = f"projects/{project_id}/search"
    elif group_id:
        endpoint = f"groups/{group_id}/search"
    else:
        endpoint = "search"

    params = {
        "scope": scope,
        "search": query,
        "search_type": search_type,
        "regex": str(regex).lower(),
        "order_by": order_by,
        "sort": sort,
    }

    # Conditional filters based on GitLab API constraints
    if state and scope in ["issues", "merge_requests"]:
        params["state"] = state
    if confidential and scope == "issues":
        params["confidential"] = str(confidential).lower()
    if fields and scope in ["issues", "merge_requests"]:
        params["fields[]"] = fields

    async with httpx.AsyncClient(timeout=30.0) as client:
        url = f"{API_URL}/{endpoint}"
        res = await client.get(url, headers={"PRIVATE-TOKEN": token}, params=params)
        res.raise_for_status()
        return res.json()
# ============================================================
# SERVER START
# ============================================================

if __name__ == "__main__":
    logger.info("Starting GitLab MCP server")
    logger.info(f"Logs directory: {LOG_DIR}")
    mcp.run(transport="http", host="0.0.0.0", port=8000)
