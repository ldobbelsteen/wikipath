import { Database, getAvailableDatabases } from "../helpers/api";
import React, { useEffect, useState } from "react";

export default function LanguageSelect(props: {
  disabled: boolean;
  selected: (tag: string) => void;
}): JSX.Element {
  const [index, setIndex] = useState(0);
  const [options, setOptions] = useState<Database[]>([]);

  // Fetch available databases and select based on the user's browser language(s)
  useEffect(() => {
    getAvailableDatabases()
      .then((databases) => {
        setOptions(databases);
        const userLanguages = navigator.languages || [navigator.language];
        for (let i = 0; i < userLanguages.length; i++) {
          const language = userLanguages[i];
          const index = databases.findIndex((database) =>
            database.languageCode.includes(language.substring(0, 2))
          );
          if (index != -1) {
            setIndex(index);
            break;
          }
        }
        return;
      })
      .catch((err) => console.error(err));
  }, []);

  // Notify language selection change when index changes
  const { selected } = props;
  useEffect(() => {
    if (options.length > index) {
      selected(options[index].languageCode);
    }
  }, [index, options, selected]);

  return (
    <select
      name="Language"
      value={index}
      disabled={props.disabled || options.length === 0}
      onChange={(event) => setIndex(parseInt(event.target.value))}
      onBlur={(event) => setIndex(parseInt(event.target.value))}
    >
      {options.map((option, index) => (
        <option key={index} value={index}>
          {option.languageName}
        </option>
      ))}
    </select>
  );
}
