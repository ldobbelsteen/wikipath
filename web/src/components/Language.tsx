import React, { useEffect, useState } from "react";
import { toast } from "react-hot-toast";
import { Database, HTTP } from "../api";

export const Language = (props: {
  disabled: boolean;
  selectedLanguageCode: string | undefined;
  setSelectedLanguageCode: (code: string | undefined) => void;
}) => {
  const { setSelectedLanguageCode } = props;

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
            database.languageCode.includes(language.substring(0, 2)),
          );
          if (supported) {
            setSelectedLanguageCode(supported.languageCode);
            return null;
          }
        }
        if (databases.length > 0) {
          setSelectedLanguageCode(databases[0].languageCode);
        }
        return null;
      })
      .finally(() => setIsLoading(false))
      .catch((err) => {
        toast.error(
          "An unexpected error occurred while getting the available languages :(",
        );
        console.error(err);
      });
  }, [setSelectedLanguageCode]);

  return (
    <select
      className="m-1 p-2"
      value={
        databases?.find(
          (database) => database.languageCode === props.selectedLanguageCode,
        )?.languageCode
      }
      disabled={props.disabled || isLoading}
      onChange={(ev) => {
        const database = databases?.find(
          (database) => database.languageCode === ev.target.value,
        );
        if (database) {
          setSelectedLanguageCode(database.languageCode);
        }
      }}
    >
      {databases?.map((database, index) => (
        <option key={index} value={database.languageCode}>
          {database.languageCode}
        </option>
      ))}
    </select>
  );
};
