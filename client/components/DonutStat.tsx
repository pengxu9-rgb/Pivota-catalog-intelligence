type DonutStatProps = {
  value: number;
  total: number;
  color: string;
};

export function DonutStat({ value, total, color }: DonutStatProps) {
  const pct = total > 0 ? Math.round((value / total) * 100) : 0;
  const fg = color;
  const bg = "#f3f4f6";

  return (
    <div className="relative w-[210px] h-[210px]">
      <div
        className="absolute inset-0 rounded-full"
        style={{
          background: `conic-gradient(${fg} ${pct}%, ${bg} 0)`,
        }}
      />
      <div className="absolute inset-[18%] rounded-full bg-white" />
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <div className="text-3xl font-bold text-gray-900">{value}</div>
        <div className="text-xs text-gray-500">of {total}</div>
        <div className="mt-1 text-xs font-medium text-gray-700">{pct}%</div>
      </div>
    </div>
  );
}

