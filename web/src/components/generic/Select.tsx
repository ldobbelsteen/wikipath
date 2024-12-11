import type { ChangeEventHandler, ReactNode } from "react";
import { twMerge } from "tailwind-merge";

export const Select = (props: {
  label: string;
  value: string;
  disabled: boolean;
  onChange: ChangeEventHandler<HTMLSelectElement>;
  options: { value: string; children: ReactNode }[];
  optionClassName?: string;
  className?: string;
}) => {
  return (
    <select
      aria-label={props.label}
      value={props.value}
      disabled={props.disabled}
      onChange={props.onChange}
      className={twMerge(
        "p-2 m-2 rounded bg-white text-black hover:brightness-75 cursor-pointer",
        props.className,
      )}
    >
      {props.options.map((opt) => (
        <option
          key={opt.value}
          value={opt.value}
          className={twMerge("cursor-pointer", props.optionClassName)}
        >
          {opt.children}
        </option>
      ))}
    </select>
  );
};
