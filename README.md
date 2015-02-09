StreamingXMLSerializer
======================
Simple, dynamic, memory efficient XML Serialization/Deserialization Library for .NET

DataSet/DataTable Serialization
-------------------------------
&nbsp;&nbsp;&nbsp;&nbsp;The reason I started this project was due to a roadblock in a project at my day job. There is data import/export functionality that allowed for the ability to import/export _n_ tables from the database, including custom tables we would know nothing about. The way this was done when I took it over was to load the entire XML file and the entire DataSet of _n_ tables into memory. Did I mention this was running on 32-bit IIS? Yikes. The ability for DataSets to be built from XML (and vice versa) is very convenient, but the performance in a real world multi-user environment is unacceptable.

The Solution
------------
_StreamingDataSet_ / _StreamingDataTable_ / _StreamingDataRow_

&nbsp;&nbsp;&nbsp;&nbsp;I attempted to recreate the relevent features of DataSets, DataTables, and DataRows with an emphasis on making everything streaming. On the XML side, this means using the _XmlReader_and_XmlWriter_ classes to read and write on a line by line basis. The output is compatible with that of _DataSet.WriteXml,_ including Schema output. On the data side, a variety of data sources can be used for input including _DataReaders_ and _IEnumerable<StreamingDataRow>_.  This allows input and output to always be trimed down to a single row in memory at any given time.

The World Without DataTables
----------------------------
&nbsp;&nbsp;&nbsp;&nbsp;From there I wanted to design a general purpose XML serializer that maintained the ability to stream the data item by item but also work eliminate some of the other limitations of standard serialization ([Serializable], Dictionary<>, etc). This is a work in progress but can currently Serialize the public properties of any object or List/IEnumerable of objects. Dictionary support is currently missing, but will be implemented soon.

Roadmap
--------
* Documentation and Examples
* Dictionary Serialization/Deserialization
* Beta Release

Requirements
-------------
.NET 4 or above (Takes advantage of Dynamic Objects)
