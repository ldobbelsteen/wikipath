import type { ChangeEventHandler, FocusEventHandler } from "react";
import { twMerge } from "tailwind-merge";

export const InputText = (props: {
  value: string;
  invalid: boolean;
  disabled: boolean;
  placeholder: string;
  onChange: ChangeEventHandler<HTMLInputElement>;
  onFocus: FocusEventHandler<HTMLInputElement>;
  onBlur: FocusEventHandler<HTMLInputElement>;
  className?: string;
}) => {
  return (
    <input
      type="text"
      value={props.value}
      placeholder={props.placeholder}
      disabled={props.disabled}
      onChange={props.onChange}
      onFocus={props.onFocus}
      onBlur={props.onBlur}
      className={twMerge(
        props.invalid
          ? "bg-red-400 text-white placeholder:text-gray-200"
          : "bg-white text-black placeholder:text-gray-400",
        "p-2 m-2 rounded",
        props.className,
      )}
    />
  );
};
