using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;

namespace OneFiftyOne.Serialization.StreamingXMLSerializer
{
    public class StreamingDataTable : IDisposable, IEnumerable<StreamingDataRow>
    {
        private DataTable schemaTable;
        internal int? count = null;

        #region CONSTRUCTORS

        internal StreamingDataTable(string baseURI, DataTable t)
        {
            if (string.IsNullOrWhiteSpace(baseURI))
                throw new Exception("Missing filename");
            if (!File.Exists(baseURI))
                throw new FileNotFoundException("File does not exist: " + baseURI);

            BaseURI = baseURI;
            schemaTable = t;
        }

        public StreamingDataTable(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new Exception("Invalid table name specified");

            schemaTable = new DataTable(tableName);
        }

        #endregion

        #region PROPERTIES

        public string BaseURI { get; private set; }

        public string TableName
        {
            get
            {
                return schemaTable.TableName;
            }
            set
            {
                if (value == string.Empty)
                    throw new Exception("Cannot set StreamingDataTable's name to be empty");
                schemaTable.TableName = value;
            }
        }

        public DataColumnCollection Columns
        {
            get
            {
                return schemaTable.Columns;
            }
        }

        public int Count
        {
            get
            {
                if (count == null)
                {
                    count = 0;
                    using (XmlReader reader = XmlReader.Create(BaseURI))
                    {
                        while (reader.ReadToFollowing(TableName))
                            count++;
                    }
                }

                return count.Value;
            }
        }

        public object DataSource { get; set; }

        #endregion

        #region READING

        private Dictionary<string, object> getNextInternal(XmlReader reader)
        {
            var obj = new Dictionary<string, object>(StringComparer.CurrentCultureIgnoreCase);

            if (reader.ReadToFollowing(TableName))
            {
                using (var subReader = reader.ReadSubtree())
                {
                    subReader.Read(); //eat parent node
                    while (subReader.Read())
                    {
                        if (subReader.NodeType == XmlNodeType.Element)
                        {
                            if (obj.ContainsKey(subReader.LocalName))
                                throw new DuplicateNameException("Key '" + subReader.LocalName + "; already exists");
                            obj[subReader.LocalName]
                                = subReader.ReadElementContentAs(schemaTable.Columns[subReader.LocalName].DataType, null);
                        }
                    }
                    subReader.Close();
                }
            }
            else
                return null;

            //normalize columns
            foreach (DataColumn c in schemaTable.Columns)
            {
                if (!obj.ContainsKey(c.ColumnName))
                    obj.Add(c.ColumnName, null);
            }

            return obj;
        }

        #endregion

        #region WRITING

        internal DataTable BuildSchemaTable()
        {
            if (DataSource == null)
                throw new Exception("DataSource cannot be NULL");

            if (DataSource is IDataReader)
                buildSchemaFromDataReader();
            else if (DataSource is IEnumerable<StreamingDataRow>)
                buildSchemaFromStreamingDataRow((DataSource as IEnumerable<StreamingDataRow>).FirstOrDefault());
            else
                throw new Exception("Invalid DataSource Type");

            return this.schemaTable;
        }

        private void buildSchemaFromStreamingDataRow(StreamingDataRow templateRow)
        {
            if (templateRow == null)
                throw new Exception("Empty Data Source");

            schemaTable.Columns.Clear();

            foreach (DataColumn c in templateRow.Columns)
                schemaTable.Columns.Add(c.ColumnName, c.DataType);
        }

        private void buildSchemaFromDataReader()
        {
            DataTable drSchema = ((IDataReader)DataSource).GetSchemaTable();
            foreach (DataRow dr in drSchema.Rows)
            {
                string columnName = dr["ColumnName"].ToString();
                Type columnType;
                if (drSchema.Columns.Contains("DataTypeName"))
                    columnType = Type.GetType(dr["DataTypeName"].ToString());
                else
                    columnType = dr["DataType"] as Type;

                DataColumn column = new DataColumn(columnName, columnType);
                schemaTable.Columns.Add(column);
            }
        }

        internal void WriteXML(XmlWriter writer)
        {
            if (DataSource == null)
                throw new Exception("DataSource cannot be NULL");

            IEnumerable<StreamingDataRow> dataRows;

            if (DataSource is IDataReader)
                dataRows = consumeReader(DataSource as IDataReader);
            else if (DataSource is IEnumerable<StreamingDataRow>)
                dataRows = DataSource as IEnumerable<StreamingDataRow>;
            else
                throw new Exception("Invalid DataSource Type");

            foreach (StreamingDataRow row in dataRows)
            {
                writer.WriteStartElement(TableName);
                foreach (DataColumn column in row.Columns)
                {
                    writer.WriteStartElement(column.ColumnName);
                    writer.WriteValue(row[column]);
                    writer.WriteEndElement();
                }
                writer.WriteEndElement();
            }
        }

        private IEnumerable<StreamingDataRow> consumeReader(IDataReader dataReader)
        {
            try
            {
                while (dataReader.Read())
                {
                    var row = new StreamingDataRow(schemaTable.Columns);
                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        if (!dataReader.IsDBNull(i))
                            row[dataReader.GetName(i)] = dataReader.GetValue(i);
                    }

                    yield return row;
                }
            }
            finally
            {
                dataReader.Close();
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            schemaTable.Dispose();
            if (DataSource != null)
            {
                if(DataSource is IDataReader)
                    ((IDataReader)DataSource).Close();
                if (DataSource is IDisposable)
                    ((IDisposable)DataSource).Dispose();
            }
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            using (XmlReader reader = XmlReader.Create(BaseURI))
            {
                while (!reader.EOF)
                {
                    var obj = getNextInternal(reader);
                    if (obj != null)
                        yield return new StreamingDataRow(schemaTable.Columns, obj);
                }
            }
        }

        IEnumerator<StreamingDataRow> IEnumerable<StreamingDataRow>.GetEnumerator()
        {
            using (XmlReader reader = XmlReader.Create(BaseURI))
            {
                while (!reader.EOF)
                {
                    var obj = getNextInternal(reader);
                    if (obj != null)
                        yield return new StreamingDataRow(schemaTable.Columns, obj);
                }
            }
        }

        #endregion
    }
}
