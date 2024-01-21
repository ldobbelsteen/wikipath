import React, { ReactNode } from "react";

export const Link = (props: { href: string; children: ReactNode }) => {
  return (
    <a href={props.href} className="hover:underline">
      {props.children}
    </a>
  );
};
