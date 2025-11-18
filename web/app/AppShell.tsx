"use client";

import type { ReactNode } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";

function navLinkClasses(pathname: string, href: string) {
  const isActive = pathname === href;
  return (
    "text-xs px-3 py-1.5 rounded-full border transition-colors " +
    (isActive
      ? "border-blue-500/80 bg-blue-500/10 text-blue-100"
      : "border-zinc-700 text-zinc-400 hover:border-zinc-500 hover:text-zinc-100")
  );
}

export default function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="min-h-screen bg-black text-white">
      {/* Top nav */}
      <header className="border-b border-zinc-900 bg-black/80 backdrop-blur-sm">
        <div className="mx-auto max-w-5xl px-4 py-3 flex items-center justify-between">
          <Link
            href="/"
            className="text-sm font-semibold tracking-tight text-zinc-100 hover:text-white"
          >
            SportIQ
          </Link>

          <nav className="flex items-center gap-2">
            <Link href="/games" className={navLinkClasses(pathname, "/games")}>
              Games
            </Link>
            <Link href="/admin" className={navLinkClasses(pathname, "/admin")}>
              Admin
            </Link>
          </nav>
        </div>
      </header>

      {/* Page content */}
      <main className="mx-auto max-w-5xl px-4 py-10">{children}</main>
    </div>
  );
}