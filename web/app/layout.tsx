import "./globals.css";
import type { ReactNode } from "react";
import type { Metadata } from "next";
import Link from "next/link";
import Image from "next/image";

export const metadata: Metadata = {
  title: "SportIQ",
  description:
    "AI-powered sports predictions with real-time win probabilities and model insights.",
};

type RootLayoutProps = {
  children: ReactNode;
};

export default function RootLayout({ children }: RootLayoutProps) {
  return (
    <html lang="en" className="h-full bg-black">
      <body className="h-full bg-black text-white">
        <div className="min-h-screen">
          {/* Top navigation bar */}
          <header className="sticky top-0 z-30 border-b border-slate-800/70 bg-[radial-gradient(circle_at_0%_0%,rgba(56,189,248,0.22),transparent_55%),radial-gradient(circle_at_100%_0%,rgba(168,85,247,0.22),transparent_55%),linear-gradient(to_right,#020617,rgba(15,23,42,0.98),#020617)] backdrop-blur-xl shadow-[0_14px_40px_rgba(0,0,0,0.75)]">
            <div className="mx-auto max-w-6xl px-6">
              <div className="flex h-16 items-center justify-between gap-4">
                {/* Logo + app name – clicking returns to home */}
                <Link
                  href="/"
                  className="group flex min-w-0 items-center gap-3"
                >
                  {/* Hero logo with radial glow */}
                  <div className="relative flex items-center justify-center">
                    {/* Soft radial halo behind logo – slightly tighter so it stays inside the bar */}
                    <div className="pointer-events-none absolute -inset-1 md:-inset-1.5 rounded-full bg-[radial-gradient(circle_at_30%_15%,rgba(56,189,248,0.22),transparent_60%),radial-gradient(circle_at_75%_85%,rgba(168,85,247,0.18),transparent_60%)] opacity-75" />
                    <div className="relative h-10 w-10 md:h-12 md:w-12">
                      {/* Subtle neon ring on hover */}
                      <div className="pointer-events-none absolute inset-0 rounded-full ring-0 ring-cyan-400/0 ring-offset-0 ring-offset-slate-950/80 transition duration-200 group-hover:ring-2 group-hover:ring-cyan-300/80 group-hover:ring-offset-2" />
                      <Image
                        src="/sportIQ-logo.jpg"
                        alt="SportIQ logo"
                        fill
                        className="rounded-full shadow-[0_0_20px_rgba(56,189,248,0.6)]"
                        priority
                      />
                    </div>
                  </div>

                  {/* Brand wordmark */}
                  <span className="relative translate-y-[0.5px] text-[1.25rem] md:text-[1.45rem] font-semibold tracking-[0.04em] bg-gradient-to-r from-cyan-200 via-teal-100 to-cyan-200 bg-clip-text text-transparent drop-shadow-[0_0_9px_rgba(56,189,248,0.25)]">
                    Sport
                    <span className="font-bold tracking-[0.06em] text-cyan-50/95 drop-shadow-[0_0_9px_rgba(56,189,248,0.4)]">
                      IQ
                    </span>
                  </span>
                </Link>

                {/* Right-side navigation with primary/secondary pills */}
                <nav className="flex items-center gap-3 md:gap-4 text-[11px] md:text-[13px] font-medium text-zinc-300">
                  {/* Primary nav item: Games (active) */}
                  <Link
                    href="/games"
                    className="relative inline-flex h-9 md:h-10 items-center justify-center overflow-hidden rounded-full px-5 md:px-6 font-semibold text-white shadow-[0_0_26px_rgba(56,189,248,0.6)] ring-1 ring-cyan-300/60 bg-gradient-to-r from-cyan-500 via-sky-500 to-purple-500 transition duration-200 hover:shadow-[0_0_34px_rgba(129,140,248,0.9)] hover:ring-cyan-200"
                  >
                    {/* Soft radial glow behind active pill */}
                    <span className="pointer-events-none absolute -inset-4 rounded-full bg-[radial-gradient(circle_at_50%_0,rgba(248,250,252,0.34),transparent_60%),radial-gradient(circle_at_50%_100%,rgba(56,189,248,0.38),transparent_65%)] opacity-90" />
                    {/* Subtle inner glass highlight */}
                    <span className="pointer-events-none absolute inset-[1px] rounded-full bg-white/5" />
                    <span className="relative">Games</span>
                  </Link>

                  {/* Secondary nav item: Admin */}
                  <Link
                    href="/admin"
                    className="relative inline-flex h-9 md:h-10 items-center justify-center rounded-full border border-slate-600/70 bg-slate-900/60 px-5 md:px-6 font-medium text-zinc-100/90 shadow-[0_0_18px_rgba(15,23,42,0.9)] transition duration-200 hover:border-cyan-300/80 hover:bg-slate-900/90 hover:shadow-[0_0_24px_rgba(56,189,248,0.6)] hover:ring-1 hover:ring-cyan-300/80 hover:ring-offset-1 hover:ring-offset-slate-950/90"
                  >
                    {/* Chromatic edge + glow on hover */}
                    <span className="pointer-events-none absolute inset-[1px] rounded-full bg-[radial-gradient(circle_at_0%_0%,rgba(56,189,248,0.22),transparent_55%),radial-gradient(circle_at_100%_100%,rgba(168,85,247,0.22),transparent_55%)] opacity-40" />
                    <span className="pointer-events-none absolute inset-0 rounded-full bg-black/20 mix-blend-multiply" />
                    <span className="relative">Admin</span>
                  </Link>
                </nav>
              </div>
            </div>
          </header>

          {/* Main page content */}
          <main className="mx-auto max-w-6xl px-6 pb-16 pt-10">{children}</main>
        </div>
      </body>
    </html>
  );
}