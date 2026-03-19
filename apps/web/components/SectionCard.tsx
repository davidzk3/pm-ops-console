import { ReactNode } from "react";

type Props = {
  title: string;
  children: React.ReactNode;
};

export default function SectionCard({ title, children }: Props) {
  return (
    <section className="rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-zinc-900">{title}</h2>
      <div>{children}</div>
    </section>
  );
}