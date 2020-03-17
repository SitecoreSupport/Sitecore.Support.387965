﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Sitecore.Configuration;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Diagnostics;
using Sitecore.ExperienceForms.Data;
using Sitecore.ExperienceForms.Data.Entities;
using Sitecore.ExperienceForms.Data.SqlServer;

namespace Sitecore.Support.ExperienceForms.Data.SqlServer
{
    public class SqlFormDataProvider : IFormDataProvider
    {
        public SqlFormDataProvider(ISqlDataApiFactory sqlServerApiFactory)
        {
            Assert.ArgumentNotNull(sqlServerApiFactory, nameof(sqlServerApiFactory));
            SqlDataApi = sqlServerApiFactory.CreateApi();
        }

        protected SqlDataApi SqlDataApi { get; }

        public IReadOnlyCollection<FormEntry> GetEntries(Guid formId, DateTime? startDate, DateTime? endDate)
        {
            DateTime? nullable = startDate;
            DateTime dateTime = (nullable.HasValue ? nullable.GetValueOrDefault() : DateTime.MinValue);
            nullable = endDate;
            DateTime dateTime1 = (nullable.HasValue ? nullable.GetValueOrDefault() : DateTime.MaxValue);
            string str = "SELECT {0}ID{1},{0}FormItemID{1},{0}Created{1} FROM {0}FormEntry{1} WHERE {0}FormItemID{1}={2}formItemId{3} AND {0}Created{1} BETWEEN {2}start{3} AND {2}end{3}";
            List<FormEntry> formEntries = new List<FormEntry>(this.SqlDataApi.CreateObjectReader<FormEntry>(str, new object[] { "formItemId", formId, "start", dateTime, "end", dateTime1 }, new Func<IDataReader, FormEntry>(SqlFormDataProvider.ParseFormEntry)));
            if (formEntries.Count == 0)

            {
                return formEntries;
            }
            string str1 = "SELECT {0}ID{1},{0}FormEntryID{1},{0}FieldItemID{1},{0}FieldName{1},{0}Value{1},{0}ValueType{1} FROM {0}FieldData{1} WHERE {0}FormEntryID{1} IN (SELECT {0}ID{1} FROM {0}FormEntry{1} WHERE {0}FormItemID{1}={2}formItemId{3} AND {0}Created{1} BETWEEN {2}start{3} AND {2}end{3})";
            IEnumerable<FieldData> fieldDatas = this.SqlDataApi.CreateObjectReader<FieldData>(str1, new object[] { "formItemId", formId, "start", dateTime, "end", dateTime1 }, new Func<IDataReader, FieldData>(SqlFormDataProvider.ParseFieldEntry));
            foreach (FieldData fieldDatum in fieldDatas)
            {
                FormEntry formEntry = formEntries.FirstOrDefault<FormEntry>((FormEntry ent) => ent.FormEntryId == fieldDatum.FormEntryId);
                if (formEntry == null)
                {
                    continue;
                }
                if (formEntry.Fields == null)
                {
                    formEntry.Fields = new List<FieldData>();
                }
                formEntry.Fields.Add(fieldDatum);
            }
            return formEntries;
        }

        public void CreateEntry(FormEntry entry)
        {
            Factory.GetRetryer().ExecuteNoResult(() =>
            {
                using (var scope = SqlDataApi.CreateTransaction())
                {
                    if (GetFormEntry(entry.FormEntryId) == null)
                    {
                        var sqlEntry = "INSERT INTO {0}FormEntry{1}" +
                            "({0}ID{1}" +
                            ",{0}FormItemID{1}" +
                            ",{0}Created{1})" +
                            "VALUES" +
                            "({2}formEntryId{3}" +
                            ",{2}formItemId{3}" +
                            ",{2}created{3});";

                        SqlDataApi.Execute(sqlEntry, "formEntryId", entry.FormEntryId, "formItemId", entry.FormItemId, "created", entry.Created);
                    }

                    if (entry.Fields != null)
                    {
                        foreach (var entryField in entry.Fields)
                        {
                            string sqlField;
                            if (GetFieldEntry(entry.FormEntryId, entryField.FieldItemId) == null)
                            {
                                sqlField = "INSERT INTO {0}FieldData{1}" +
                                    "({0}ID{1}" +
                                    ",{0}FormEntryID{1}" +
                                    ",{0}FieldItemID{1}" +
                                    ",{0}FieldName{1}" +
                                    ",{0}Value{1}" +
                                    ",{0}ValueType{1})" +
                                    "VALUES" +
                                    "({2}fieldEntryId{3}" +
                                    ",{2}formEntryId{3}" +
                                    ",{2}fieldItemId{3}" +
                                    ",{2}fieldName{3}" +
                                    ",{2}fieldValue{3}" +
                                    ",{2}fieldValueType{3});";
                            }
                            else
                            {
                                sqlField = "UPDATE {0}FieldData{1}" +
                                    "SET {0}FieldName{1}={2}fieldName{3}" +
                                    ",{0}Value{1}={2}fieldValue{3}" +
                                    ",{0}ValueType{1}={2}fieldValueType{3}" +
                                    " WHERE {0}FormEntryID{1}={2}formEntryId{3}" +
                                    " AND {0}FieldItemID{1}={2}fieldItemId{3}";
                            }

                            SqlDataApi.Execute(sqlField,
                                "fieldEntryId", entryField.FieldDataId,
                                "formEntryId", entry.FormEntryId,
                                "fieldItemId", entryField.FieldItemId,
                                "fieldName", entryField.FieldName,
                                "fieldValue", entryField.Value,
                                "fieldValueType", entryField.ValueType);
                        }
                    }

                    scope.Complete();
                }
            });
        }

        public void DeleteEntries(Guid formId)
        {
            var sql = "DELETE FROM {0}FormEntry{1} WHERE {0}FormItemID{1}={2}formItemId{3}";

            Factory.GetRetryer().ExecuteNoResult(() =>
            {
                using (var scope = SqlDataApi.CreateTransaction())
                {
                    SqlDataApi.Execute(sql, "formItemId", formId);
                    scope.Complete();
                }
            });
        }

        protected virtual FormEntry GetFormEntry(Guid formEntryId)
        {
            var sqlForm = "SELECT {0}ID{1}" +
                ",{0}FormItemID{1}" +
                ",{0}Created{1}" +
                " FROM {0}FormEntry{1}" +
                " WHERE {0}ID{1}={2}formEntryId{3}";

            var entries = SqlDataApi.CreateObjectReader(sqlForm, new object[]
            {
                "formEntryId",
                formEntryId
            }, ParseFormEntry);

            return entries.FirstOrDefault();
        }

        protected virtual FieldData GetFieldEntry(Guid formEntryId, Guid fieldItemId)
        {
            var sqlFields = "SELECT {0}ID{1}" +
                ",{0}FormEntryID{1}" +
                ",{0}FieldItemID{1}" +
                ",{0}FieldName{1}" +
                ",{0}Value{1}" +
                ",{0}ValueType{1}" +
                " FROM {0}FieldData{1}" +
                " WHERE {0}FormEntryID{1}={2}formEntryId{3}" +
                " AND {0}FieldItemID{1}={2}fieldItemId{3}";

            var entries = SqlDataApi.CreateObjectReader(sqlFields, new object[]
            {
                "formEntryId",
                formEntryId,
                "fieldItemId",
                fieldItemId
            }, ParseFieldEntry);

            return entries.FirstOrDefault();
        }

        private static FormEntry ParseFormEntry(IDataReader dataReader)
        {
            var entryObject = new FormEntry
            {
                FormEntryId = dataReader.GetGuid(0),
                FormItemId = dataReader.GetGuid(1),
                Created = dataReader.GetDateTime(2)
            };

            return entryObject;
        }

        private static FieldData ParseFieldEntry(IDataReader dataReader)
        {
            var entryObject = new FieldData
            {
                FieldDataId = dataReader.GetGuid(0),
                FormEntryId = dataReader.GetGuid(1),
                FieldItemId = dataReader.GetGuid(2),
                FieldName = dataReader.GetString(3),
                Value = dataReader.GetString(4),
                ValueType = dataReader.GetString(5)
            };

            return entryObject;
        }
    }
}
