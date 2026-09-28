"""Reusable, source-agnostic ingestion helpers."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


TRANSIENT_HTTP_CODES = (
    429,
    500,
    502,
    503,
    504,
)


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def make_run_id(
    now: datetime | None = None,
) -> str:
    """Return a sortable, collision-resistant run identifier."""

    instant = now or utc_now()

    return (
        f"{instant:%Y%m%dT%H%M%SZ}_"
        f"{uuid.uuid4().hex[:8]}"
    )


def retry_session(
    user_agent: str,
    retries: int = 4,
) -> requests.Session:
    """Create a GET/POST session with bounded retries and backoff."""

    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        allowed_methods=frozenset(
            {
                "GET",
                "POST",
            }
        ),
        status_forcelist=TRANSIENT_HTTP_CODES,
        backoff_factor=1.0,
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry
    )

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": user_agent,
        }
    )

    session.mount(
        "https://",
        adapter,
    )

    session.mount(
        "http://",
        adapter,
    )

    return session


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, object] | None = None,
    body: dict | None = None,
    timeout: tuple[int, int] = (10, 90),
) -> tuple[object, bytes]:
    """
    Request JSON and return both parsed content and exact response bytes.

    Normal JSON parsing is attempted first.

    If the source includes a UTF-8 BOM, retry using utf-8-sig
    so the raw response can still be parsed without modifying
    the original bytes.
    """

    response = session.request(
        method,
        url,
        params=params,
        json=body,
        timeout=timeout,
    )

    response.raise_for_status()

    raw = response.content

    if not raw:
        raise ValueError(
            f"Empty response from {response.url}"
        )

    try:
        return response.json(), raw

    except requests.exceptions.JSONDecodeError:

        try:
            payload = json.loads(
                raw.decode("utf-8-sig")
            )

            return payload, raw

        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as error:

            raise ValueError(
                f"Invalid JSON from {response.url}"
            ) from error


def value_at_path(
    payload: object,
    path: tuple[str, ...],
) -> object:
    """Read a required nested JSON value."""

    value = payload

    for key in path:

        if (
            not isinstance(value, dict)
            or key not in value
        ):
            raise ValueError(
                "Missing JSON path: "
                f"{'.'.join(path)}"
            )

        value = value[key]

    return value


def record_array(
    payload: object,
    path: tuple[str, ...],
) -> list[dict]:
    """Return a verified list of objects at one JSON path."""

    value = value_at_path(
        payload,
        path,
    )

    if not isinstance(value, list):
        raise TypeError(
            f"Expected {'.'.join(path)} "
            "to be a list"
        )

    if any(
        not isinstance(item, dict)
        for item in value
    ):
        raise TypeError(
            f"Expected every {'.'.join(path)} "
            "item to be an object"
        )

    return value


def ensure_directory(
    path: str | Path,
) -> Path:
    """Create and return one directory."""

    directory = Path(path)

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    return directory


def write_bytes(
    path: str | Path,
    raw: bytes,
) -> None:
    """Atomically write one raw source artifact."""

    destination = Path(path)

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = destination.with_suffix(
        destination.suffix + ".tmp"
    )

    temporary.write_bytes(raw)

    temporary.replace(destination)


def write_json(
    path: str | Path,
    value: object,
) -> None:
    """Atomically write a small JSON manifest."""

    raw = json.dumps(
        value,
        indent=2,
        sort_keys=True,
        default=str,
    ).encode("utf-8")

    write_bytes(
        path,
        raw,
    )


def sha256(
    raw: bytes,
) -> str:
    """Return the SHA-256 digest for source evidence."""

    return hashlib.sha256(
        raw
    ).hexdigest()


def sha256_file(
    path: str | Path,
) -> str:
    """Hash a potentially large source file without loading it all in memory."""

    digest = hashlib.sha256()

    with Path(path).open("rb") as stream:

        for chunk in iter(
            lambda: stream.read(
                1024 * 1024
            ),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def discover_files(
    directory: str | Path,
    suffixes: set[str],
) -> list[Path]:
    """Discover supported source files recursively and deterministically."""

    root = Path(directory)

    if not root.is_dir():
        raise FileNotFoundError(
            "Landing directory does not exist: "
            f"{root}"
        )

    files = sorted(
        path
        for path in root.rglob("*")
        if (
            path.is_file()
            and not path.name.startswith(
                (
                    "~$",
                    ".",
                )
            )
            and path.suffix.casefold()
            in suffixes
        )
    )

    if not files:
        raise FileNotFoundError(
            "No "
            f"{', '.join(sorted(suffixes))} "
            f"files found under {root}"
        )

    return files


def normalize_header(
    value: object,
) -> str:
    """Normalize an Excel header only for contract comparison."""

    return re.sub(
        r"[^a-z0-9]+",
        " ",
        str(value).casefold(),
    ).strip()


def _match_headers(
    headers: list[object],
    fields: dict[str, list[str]],
    *,
    required: bool,
) -> dict[str, str | None] | None:
    """Match reviewed canonical fields against workbook headers."""

    normalized_to_actual: dict[
        str,
        str,
    ] = {}

    for header in headers:

        normalized = normalize_header(
            header
        )

        if not normalized:
            continue

        if normalized in normalized_to_actual:
            return None

        normalized_to_actual[
            normalized
        ] = str(header).strip()


    matched: dict[
        str,
        str | None,
    ] = {}

    for canonical, aliases in fields.items():

        candidates = {
            normalized_to_actual[
                normalize_header(alias)
            ]
            for alias in aliases
            if normalize_header(alias)
            in normalized_to_actual
        }

        if len(candidates) > 1:
            raise ValueError(
                "More than one source column "
                f"matched {canonical}: "
                f"{sorted(candidates)}"
            )

        if not candidates:

            if required:
                return None

            matched[
                canonical
            ] = None

        else:
            matched[
                canonical
            ] = candidates.pop()

    return matched


def inspect_excel_contract(
    source_file: Path,
    contract: dict,
) -> tuple[
    str,
    int,
    dict[str, str | None],
]:
    """Find exactly one sheet and header row matching a reviewed contract."""

    candidates: list[
        tuple[
            str,
            int,
            dict[str, str | None],
        ]
    ] = []

    scan_rows = int(
        contract["scan_rows"]
    )

    with pd.ExcelFile(
        source_file
    ) as workbook:

        for sheet_name in workbook.sheet_names:

            preview = workbook.parse(
                sheet_name=sheet_name,
                header=None,
                nrows=scan_rows,
                dtype=str,
                keep_default_na=False,
            )

            for (
                header_row,
                row,
            ) in preview.iterrows():

                headers = row.tolist()

                required = _match_headers(
                    headers,
                    contract["required"],
                    required=True,
                )

                if required is None:
                    continue

                optional = _match_headers(
                    headers,
                    contract.get(
                        "optional",
                        {},
                    ),
                    required=False,
                )

                candidates.append(
                    (
                        sheet_name,
                        int(header_row),
                        {
                            **required,
                            **(
                                optional
                                or {}
                            ),
                        },
                    )
                )


    if len(candidates) != 1:

        layouts = [
            (
                sheet,
                row,
            )
            for (
                sheet,
                row,
                _,
            ) in candidates
        ]

        raise ValueError(
            "Expected one matching PSGC layout "
            f"in {source_file.name}; "
            f"found {layouts}"
        )

    return candidates[0]


def read_excel_contract(
    source_file: Path,
    contract: dict,
) -> tuple[
    pd.DataFrame,
    dict,
]:
    """Read one contract-matched workbook into canonical string columns."""

    (
        sheet_name,
        header_row,
        mapping,
    ) = inspect_excel_contract(
        source_file,
        contract,
    )


    frame = pd.read_excel(
        source_file,
        sheet_name=sheet_name,
        header=header_row,
        dtype=str,
        keep_default_na=False,
    )


    frame.columns = [
        str(column).strip()
        for column in frame.columns
    ]


    canonical = pd.DataFrame()


    for (
        target,
        source,
    ) in mapping.items():

        canonical[target] = (
            frame[source]
            .astype(str)
            .str.strip()
            if source
            else ""
        )


    canonical[
        "source_record_json"
    ] = frame.apply(
        lambda row: json.dumps(
            {
                str(key): str(value)
                for (
                    key,
                    value,
                ) in row.items()
            },
            ensure_ascii=False,
        ),
        axis=1,
    )


    canonical = canonical[
        canonical[
            "psgc_code"
        ]
        .str.strip()
        .ne("")
    ].copy()


    canonical[
        "source_row_number"
    ] = range(
        header_row + 2,
        header_row
        + 2
        + len(canonical),
    )


    metadata = {
        "sheet_name": sheet_name,
        "header_row": header_row,
        "mapping": mapping,
        "records": len(canonical),
    }


    return (
        canonical.reset_index(
            drop=True
        ),
        metadata,
    )