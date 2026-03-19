type Row = {
  label: string;
  value: React.ReactNode;
};

type Props = {
  rows: Row[];
};

export default function KeyValueTable({ rows }: Props) {
  return (
    <div className="overflow-hidden rounded-xl border border-zinc-200">
      <table className="w-full text-sm">
        <tbody>
          {rows.map((row) => (
            <tr key={row.label} className="border-b border-zinc-200 last:border-b-0">
              <td className="w-1/3 bg-zinc-50 px-4 py-3 font-medium text-zinc-700">
                {row.label}
              </td>
              <td className="px-4 py-3 text-zinc-900">{row.value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}