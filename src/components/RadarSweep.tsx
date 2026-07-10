/** Elemen signature: sapuan radar berputar, meniru layar konsol ATC sungguhan.
 * Murni CSS (conic-gradient + animasi rotate), diletakkan di atas peta. */
export function RadarSweep() {
  return (
    <div className="pointer-events-none absolute inset-0 overflow-hidden">
      <div
        className="absolute left-1/2 top-1/2 h-[200%] w-[200%] -translate-x-1/2 -translate-y-1/2 animate-radar-spin opacity-[0.15]"
        style={{
          background:
            "conic-gradient(from 0deg, transparent 0deg, transparent 300deg, var(--color-phosphor) 358deg, transparent 360deg)",
        }}
      />
      <div className="absolute inset-0 [background-image:radial-gradient(circle,transparent_0%,transparent_60%,var(--color-void)_100%)]" />
    </div>
  );
}
