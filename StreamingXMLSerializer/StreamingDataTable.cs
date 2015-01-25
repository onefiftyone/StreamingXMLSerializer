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
        private static XmlWriterSettings xmlWriterSettings = new XmlWriterSettings()
        {
            Indent = true,
            IndentChars = "\t"
        };

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
                if (schemaTable.Columns.Count == 0 && DataSource != null)
                    BuildSchemaTable();

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

        public void ReadXML(string filename)
        {
            BaseURI = filename;
            schemaTable = new DataTable();
            using (XmlReader reader = XmlReader.Create(BaseURI))
            {
                reader.MoveToContent();
                if (!reader.ReadToFollowing("xs:schema"))
                    throw new InvalidOperationException("StreamingDataTable does not support XML files with missing schema information");

                using (var subReader = reader.ReadSubtree())
                {
                    schemaTable.ReadXmlSchema(subReader);
                    subReader.Close();
                }
                reader.Close();
            }
        }

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

                            //nil/null check
                            var nilAttr = subReader.GetAttribute("xsi:nil");
                            if (!string.IsNullOrWhiteSpace(nilAttr) && nilAttr.Equals("true", StringComparison.CurrentCultureIgnoreCase))
                                obj[subReader.LocalName] = null;
                            else
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

        public void WriteXML(string file, bool omitXmlDeclaration = true)
        {
            schemaTable = BuildSchemaTable();

            xmlWriterSettings.OmitXmlDeclaration = omitXmlDeclaration;
            using (var writer = XmlWriter.Create(file, xmlWriterSettings))
            {
                //write root element
                writer.WriteStartElement("NewDataSet");

                //write schema
                schemaTable.WriteXmlSchema(writer);

                //write contents
                this.WriteXML(writer);

                //write end root element
                writer.WriteEndElement();
            }
        }

        internal void WriteXML(XmlWriter writer)
        {
            if (DataSource == null)
                throw new Exception("DataSource cannot be NULL");

            //resolve data source if its a Delegate
            object resolvedDataSource = null;
            if (DataSource is Delegate)
                resolvedDataSource = ((Delegate)DataSource).DynamicInvoke();
            else
                resolvedDataSource = DataSource;

            IEnumerable<StreamingDataRow> dataRows;

            if (resolvedDataSource is IDataReader)
                dataRows = consumeReader(resolvedDataSource as IDataReader);
            else if (resolvedDataSource is IEnumerable<StreamingDataRow>)
                dataRows = resolvedDataSource as IEnumerable<StreamingDataRow>;
            else if (resolvedDataSource is StreamingDataRow)
                dataRows = new List<StreamingDataRow> { resolvedDataSource as StreamingDataRow };
            else
                throw new Exception("Invalid DataSource Type");

            foreach (StreamingDataRow row in dataRows)
            {
                writer.WriteStartElement(TableName);
                foreach (DataColumn column in row.Columns)
                {
                    if (row[column] != null) // skip element if value is null
                    {
                        writer.WriteStartElement(column.ColumnName);
                        writer.WriteValue(row[column]);
                        writer.WriteEndElement();
                    }
                }
                writer.WriteEndElement();
            }
        }


        internal DataTable BuildSchemaTable()
        {
            if (DataSource == null)
                throw new Exception("DataSource cannot be NULL");


            schemaTable.Columns.Clear();

            //resolve data source if its a Delegate
            object resolvedDataSource = null;
            if (DataSource is Delegate)
                resolvedDataSource = ((Delegate)DataSource).DynamicInvoke();
            else
                resolvedDataSource = DataSource;

            if (resolvedDataSource is IDataReader)
                buildSchemaFromDataReader(resolvedDataSource as IDataReader);
            else if (resolvedDataSource is IEnumerable<StreamingDataRow>)
                buildSchemaFromStreamingDataRow((resolvedDataSource as IEnumerable<StreamingDataRow>).FirstOrDefault());
            else if (resolvedDataSource is StreamingDataRow)
                buildSchemaFromStreamingDataRow(resolvedDataSource as StreamingDataRow);
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

        private void buildSchemaFromDataReader(IDataReader dataReader)
        {
            DataTable drSchema = dataReader.GetSchemaTable();
            foreach (DataRow dr in drSchema.Rows)
            {
                string columnName = dr["ColumnName"].ToString();
                Type columnType = dr["DataType"] as Type;
                DataColumn column = new DataColumn(columnName, columnType);
                schemaTable.Columns.Add(column);
            }

            if (DataSource is Delegate)
            {
                dataReader.Close();
                dataReader.Dispose();
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
                if (DataSource is Delegate)
                {
                    dataReader.Close();
                    dataReader.Dispose();
                }
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            schemaTable.Dispose();

            if (DataSource != null)
            {
                if (DataSource is IDataReader)
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
