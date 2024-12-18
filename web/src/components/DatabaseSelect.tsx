import { useEffect, useState } from "react";
import { toast } from "react-toastify";
import { listDatabases } from "../api";
import type { Database } from "../schema";
import { Select } from "./generic/Select";

/** Select the best language based on the user's browser languages. */
export const defaultDatabase = (databases: Database[]) => {
  for (const browserLanguage of navigator.languages) {
    for (const database of databases) {
      if (database.languageCode.includes(browserLanguage.substring(0, 2))) {
        return database;
      }
    }
  }

  if (databases.length > 0) {
    return databases[0];
  }
  toast.error("No languages available on this server");
};

export const DatabaseSelect = (props: {
  disabled: boolean;
  selected: Database | undefined;
  setSelected: (v: Database | undefined) => void;
}) => {
  const [databases, setDatabases] = useState<Database[]>();

  const { setSelected } = props;

  /**
   * Fetch available databases and select default.
   */
  useEffect(() => {
    listDatabases()
      .then((databases) => {
        setDatabases(databases);
        setSelected(defaultDatabase(databases));
        return null;
      })
      .catch((err: unknown) => {
        toast.error(
          "An unexpected error occurred while getting the available languages",
        );
        console.error(err);
      });
  }, [setSelected]);

  return (
    databases !== undefined &&
    props.selected !== undefined && (
      <Select
        label="Select Wikipedia language"
        disabled={props.disabled}
        value={props.selected.languageCode}
        onChange={(ev) => {
          setSelected(
            databases.find(
              (database) => database.languageCode === ev.target.value,
            ),
          );
        }}
        options={databases.map((database) => ({
          value: database.languageCode,
          children: <>{database.languageCode}</>,
        }))}
      />
    )
  );
};
