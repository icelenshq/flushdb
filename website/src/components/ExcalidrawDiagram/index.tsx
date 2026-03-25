import React, {useEffect, useRef, useState} from 'react';
import BrowserOnly from '@docusaurus/BrowserOnly';
import {useColorMode} from '@docusaurus/theme-common';

interface ExcalidrawDiagramProps {
  file: string;
  height?: number;
}

function ExcalidrawViewer({file, height = 500}: ExcalidrawDiagramProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const {colorMode} = useColorMode();

  useEffect(() => {
    let cancelled = false;

    async function render() {
      try {
        const [mod, res] = await Promise.all([
          import('@excalidraw/excalidraw'),
          fetch(file),
        ]);

        if (cancelled) return;

        if (!res.ok) {
          setError(`Failed to load diagram: ${res.status}`);
          return;
        }

        const data = await res.json();
        if (cancelled) return;

        const svg = await mod.exportToSvg({
          elements: data.elements,
          appState: {
            ...data.appState,
            exportWithDarkMode: colorMode === 'dark',
            exportBackground: false,
          },
          files: data.files ?? null,
        });

        if (cancelled || !containerRef.current) return;

        svg.style.width = '100%';
        svg.style.height = '100%';
        containerRef.current.innerHTML = '';
        containerRef.current.appendChild(svg);
        setLoaded(true);
      } catch (e) {
        if (!cancelled) setError(String(e));
      }
    }

    render();
    return () => {
      cancelled = true;
    };
  }, [file, colorMode]);

  return (
    <div
      style={{
        height,
        marginBottom: '1.5rem',
        border: '1px solid var(--ifm-color-emphasis-300)',
        borderRadius: 8,
        overflow: 'hidden',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}>
      {error ? (
        <span style={{color: 'var(--ifm-color-danger)'}}>{error}</span>
      ) : !loaded ? (
        <span>Loading diagram...</span>
      ) : null}
      <div ref={containerRef} style={{width: '100%', height: '100%'}} />
    </div>
  );
}

export default function ExcalidrawDiagram(props: ExcalidrawDiagramProps) {
  return (
    <BrowserOnly fallback={<div>Loading diagram...</div>}>
      {() => <ExcalidrawViewer {...props} />}
    </BrowserOnly>
  );
}
