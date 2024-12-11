import type { MouseEventHandler, ReactNode } from "react";
import { twMerge } from "tailwind-merge";

export const Button = (props: {
  disabled?: boolean;
  onClick?: MouseEventHandler<HTMLButtonElement>;
  onMouseDown?: MouseEventHandler<HTMLButtonElement>;
  children: ReactNode;
  className?: string;
}) => {
  return (
    <button
      type="button"
      disabled={props.disabled}
      onClick={props.onClick}
      onMouseDown={props.onMouseDown}
      className={twMerge(
        "p-2 m-2 rounded bg-white text-black hover:brightness-75",
        props.className,
      )}
    >
      {props.children}
    </button>
  );
};
