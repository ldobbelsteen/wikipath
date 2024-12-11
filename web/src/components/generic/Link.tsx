import type { ReactNode } from "react";
import { twMerge } from "tailwind-merge";

export const Link = (props: {
  href: string;
  children: ReactNode;
  className?: string;
}) => {
  return (
    <a href={props.href} className={twMerge("underline", props.className)}>
      {props.children}
    </a>
  );
};
