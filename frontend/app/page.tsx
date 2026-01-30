"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { ERDViewer } from "@/components/erd-viewer"

// Types matching Backend
interface ColumnSchema {
  name: string
  type: string
  is_pk: boolean
  is_fk: boolean
  fk_table?: string
  fk_column?: string
}

interface TableSchema {
  name: string
  columns: ColumnSchema[]
  is_root: boolean
}

interface SchemaMap {
  tables: TableSchema[]
}

export default function Home() {
  const [file, setFile] = useState<File | null>(null)
  const [connectionString, setConnectionString] = useState("")
  const [schemaMap, setSchemaMap] = useState<SchemaMap | null>(null)
  const [jsonData, setJsonData] = useState<any>(null)
  const [mermaidChart, setMermaidChart] = useState<string>("")
  const [syncStatus, setSyncStatus] = useState<string>("")
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    fetch("http://localhost:8000/config")
      .then((res) => res.json())
      .then((data) => {
        if (data.connection_string) {
          setConnectionString(data.connection_string)
        }
      })
      .catch((err) => console.error("Failed to fetch config:", err))
  }, [])

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setFile(e.target.files[0])
    }
  }

  const generateMermaidChart = (schema: SchemaMap) => {
    // Helper to sanitize entity names for Mermaid ERD
    const escapeName = (name: string) => {
      // Replace special characters with underscores for Mermaid compatibility
      return name.replace(/[:\s\-\.]/g, "_")
    }

    let chart = "erDiagram\n"
    schema.tables.forEach((table) => {
      const escapedName = escapeName(table.name)
      chart += `    ${escapedName} {\n`
      table.columns.forEach((col) => {
        // Mermaid types: string, int, float, etc.
        // We can just use the SQL type or simplify
        // fix: 'INT IDENTITY(1,1)' -> 'int'
        let type = col.type.split("(")[0].trim().split(" ")[0].toLowerCase()
        const escapedColName = escapeName(col.name)

        chart += `        ${type} ${escapedColName}`
        if (col.is_pk) chart += " PK"
        if (col.is_fk) chart += " FK"
        chart += "\n"
      })
      chart += `    }\n`
    })

    // Relationships
    schema.tables.forEach((table) => {
      table.columns.forEach((col) => {
        if (col.is_fk && col.fk_table) {
          // table }|..|| fk_table : "related"
          // actually Child }|--|| Parent usually
          // For visualization: Parent ||--|{ Child
          const parent = escapeName(col.fk_table)
          const child = escapeName(table.name)
          chart += `    ${parent} ||--o{ ${child} : "has"\n`
        }
      })
    })
    return chart
  }

  const handleUpload = async () => {
    if (!file) return
    setLoading(true)
    const formData = new FormData()
    formData.append("file", file)

    try {
      const res = await fetch("http://localhost:8000/process-json", {
        method: "POST",
        body: formData,
      })
      if (!res.ok) throw new Error("Failed to process")
      const data = await res.json()
      setSchemaMap(data.schema_map)
      setJsonData(data.json_data)
      setMermaidChart(generateMermaidChart(data.schema_map))
    } catch (err: any) {
      alert("Error: " + err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleSync = async () => {
    if (!connectionString || !schemaMap || !jsonData) {
      alert("Missing connection string or data")
      return
    }
    setLoading(true)
    setSyncStatus("Syncing...")
    try {
      const res = await fetch("http://localhost:8000/sync", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          connection_string: connectionString,
          schema_map: schemaMap,
          json_data: jsonData,
        }),
      })
      if (!res.ok) throw new Error(await res.text())
      const result = await res.json()
      setSyncStatus(`Success: ${JSON.stringify(result.rows_inserted)}`)
    } catch (err: any) {
      setSyncStatus("Error: " + err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="container mx-auto p-4 space-y-8">
      <h1 className="text-3xl font-bold">JSON to SQL Server Migrator</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card>
          <CardHeader>
            <CardTitle>1. Upload & Analyze</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <Input type="file" accept=".json" onChange={handleFileChange} />
            <Button onClick={handleUpload} disabled={!file || loading}>
              {loading ? "Processing..." : "Analyze JSON"}
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>3. Sync to Database</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <Input
              placeholder="Connection String (e.g. mssql+pyodbc://...)"
              value={connectionString}
              onChange={(e) => setConnectionString(e.target.value)}
            />
            <Button onClick={handleSync} disabled={!schemaMap || loading}>
              {loading ? "Syncing..." : "Sync to SQL Server"}
            </Button>
            {syncStatus && (
              <div className="p-2 bg-gray-100 rounded text-sm font-mono mt-2 break-all">
                {syncStatus}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {mermaidChart && (
        <Card>
          <CardHeader>
            <CardTitle>2. Schema Preview</CardTitle>
          </CardHeader>
          <CardContent>
            <ERDViewer chart={mermaidChart} />
          </CardContent>
        </Card>
      )}
    </div>
  )
}
