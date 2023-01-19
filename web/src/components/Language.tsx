import React, { useEffect, useState } from "react";
import { toast } from "react-hot-toast";
import { Database, HTTP } from "../api";

export const Language = (props: {
  disabled: boolean;
  selectedlangCode: string | undefined;
  setSelectedlangCode: (code: string | undefined) => void;
}) => {
  const { setSelectedlangCode } = props;

  const [isLoading, setIsLoading] = useState(true);
  const [databases, setDatabases] = useState<Database[]>();

  /**
   * Fetch available databases and select based on the user's browser
   * language(s)
   */
  useEffect(() => {
    HTTP.listDatabases()
      .then((databases) => {
        setDatabases(databases);
        for (const language of navigator.languages) {
          const supported = databases.find((database) =>
            database.langCode.includes(language.substring(0, 2))
          );
          if (supported) {
            setSelectedlangCode(supported.langCode);
            break;
          }
        }
        return null;
      })
      .finally(() => setIsLoading(false))
      .catch((err) => {
        toast.error(
          "An unexpected error occurred while getting the available languages :("
        );
        console.error(err);
      });
  }, [setSelectedlangCode]);

  return (
    <select
      className="m-1 p-2"
      value={
        databases?.find(
          (database) => database.langCode === props.selectedlangCode
        )?.langCode
      }
      disabled={props.disabled || isLoading}
      onChange={(ev) => {
        const database = databases?.find(
          (database) => database.langCode === ev.target.value
        );
        if (database) {
          setSelectedlangCode(database.langCode);
        }
      }}
    >
      {databases?.map((database, index) => (
        <option key={index} value={database.langCode}>
          {database.langCode}
        </option>
      ))}
    </select>
  );
};
