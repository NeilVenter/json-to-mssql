"use client"
import React, { useEffect, useRef, useState } from "react"
import mermaid from "mermaid"

interface ERDViewerProps {
    chart: string
}

export function ERDViewer({ chart }: ERDViewerProps) {
    const [svg, setSvg] = useState<string>("")
    const ref = useRef<HTMLDivElement>(null)

    useEffect(() => {
        if (chart) {
            mermaid.initialize({ startOnLoad: false, theme: "default" })
            mermaid.render("mermaid-chart", chart).then((res) => {
                setSvg(res.svg)
            })
        }
    }, [chart])

    return (
        <div className="w-full overflow-auto p-4 bg-white rounded border border-gray-200">
            {svg ? (
                <div dangerouslySetInnerHTML={{ __html: svg }} />
            ) : (
                <div className="text-gray-400">Waiting for schema...</div>
            )}
        </div>
    )
}
