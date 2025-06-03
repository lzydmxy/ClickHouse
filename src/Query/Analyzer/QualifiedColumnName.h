#pragma once

#include <Core/QualifiedTableName.h>
#include <string>


namespace DB
{

    struct QualifiedColumnName
    {
        std::string database;
        std::string table;
        std::string column;

        QualifiedColumnName(QualifiedColumnName &&) = default;
        QualifiedColumnName(const QualifiedColumnName &) = default;
        QualifiedColumnName & operator=(QualifiedColumnName &&) = default;
        QualifiedColumnName & operator=(const QualifiedColumnName &) = default;

        explicit QualifiedColumnName(const std::string & _database, const std::string & _table, const std::string & _column)
            : database(_database), table(_table), column(_column)
        {
        }

        bool operator<(const QualifiedColumnName & other) const
        {
            return std::forward_as_tuple(database, table, column) < std::forward_as_tuple(other.database, other.table, other.column);
        }

        bool operator==(const QualifiedColumnName & other) const
        {
            return database == other.database && table == other.table && column == other.column;
        }

        std::size_t hash() const;

        std::string getFullName() const { return database + "." + table + "." + column; }

        QualifiedTableName getQualifiedTable() const { return QualifiedTableName{database, table}; }
    };

    struct QualifiedColumnNameHash
    {
        std::size_t operator()(const QualifiedColumnName & column_name) const { return column_name.hash(); }
    };

}
