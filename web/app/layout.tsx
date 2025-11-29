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
          <header
            className="relative z-30 border-b border-slate-800/80 bg-black/70 backdrop-blur-sm" // subtle blur + darker separator
          >
            <div
              className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4 md:py-6" // a bit taller for a more spacious feel
            >
              {/* Logo + app name – clicking returns to home */}
              <Link
                href="/"
                className="flex items-center gap-3.5 md:gap-4"
              >
                {/* Larger, cleaner logo */}
                <div className="relative h-10 w-10 md:h-12 md:w-12">
                  <Image
                    src="/sportiq-logo.png"
                    alt="SportIQ logo"
                    fill
                    className="rounded-full shadow-[0_0_20px_rgba(59,130,246,0.35)]" // soft glow around logo
                    priority
                  />
                </div>
                {/* Larger, more prominent brand text */}
                <span className="text-lg md:text-xl font-semibold tracking-tight">
                  SportIQ
                </span>
              </Link>

              {/* Right-side navigation with primary/secondary pills */}
              <nav className="flex items-center gap-2.5 md:gap-3.5 text-xs md:text-sm font-medium text-zinc-300">
                {/* Primary nav item: Games */}
                <Link
                  href="/games"
                  className="rounded-full bg-blue-500 px-5 md:px-6 py-1.5 md:py-2.5 text-xs md:text-sm font-semibold text-white shadow-md shadow-blue-500/40 transition hover:bg-blue-400 hover:shadow-blue-400/70"
                >
                  Games
                </Link>

                {/* Secondary nav item: Admin */}
                <Link
                  href="/admin"
                  className="rounded-full border border-slate-600 px-5 md:px-6 py-1.5 md:py-2.5 text-xs md:text-sm font-medium text-zinc-100 transition hover:border-slate-300 hover:bg-white/5"
                >
                  Admin
                </Link>
              </nav>
            </div>

            {/* subtle gradient line under navbar to make it feel like a distinct layer */}
            <div className="pointer-events-none absolute inset-x-0 bottom-0 h-px bg-gradient-to-r from-transparent via-blue-500/60 to-transparent" />
          </header>

          {/* Main page content */}
          <main className="mx-auto max-w-6xl px-6 pb-16 pt-10">{children}</main>
        </div>
      </body>
    </html>
  );
}