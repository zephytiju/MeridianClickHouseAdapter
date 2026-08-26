<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security policy

Please report suspected vulnerabilities privately through GitHub Security Advisories for
`zephytiju/meridian-storage-clickhouse`. Do not include credentials, endpoints, tenant data,
query results, or other sensitive material in a public issue.

Only the latest released minor version receives security fixes. Runtime configuration accepts
opaque secret references through Meridian Core; this package never accepts secret bytes in
consumer Expressions or serialized Operations.
