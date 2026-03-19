import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "PM Ops Console",
  description: "Structural intelligence console for prediction markets",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="bg-zinc-50 text-zinc-900 antialiased">
        <div className="min-h-screen">
          <header className="border-b border-zinc-200 bg-white">
            <div className="mx-auto max-w-7xl px-6 py-4">
              <div>
                <h1 className="text-xl font-bold">PM Ops Console</h1>
                <p className="text-sm text-zinc-500">
                  Structural intelligence for prediction markets
                </p>
              </div>
            </div>
          </header>

          <main className="mx-auto max-w-7xl px-6 py-8">{children}</main>
        </div>
      </body>
    </html>
  );
}