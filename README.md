# varilog
CERN Varilog 2.0

User-domain persistency layer for the CERN Injectors' control system.

The project's key compoenents are:
  - **Cassandra.Varilog** Data model and permanent storage based on the Apache Cassandra NoSQL database;
  - **vrl** A command-line tool written in Python to manage the data, users, tagging metadata and interact with Cassandra.Varilog;
  - **varilog** A package providing the Python API to the functionalities of the varilog. Queries can be run either directly with CQL (Cassandra Query Language) or *via* the complete Object Model of Cassandra.Varilog.

The project has its own Github hosted website: http://chernals.github.io/varilog/ .
  
The source code, documentation, support and examples are all hosted on Github (http://www.github.com/chernals/varilog).
