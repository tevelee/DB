import DuckDB
import Foundation
import TabularData

final class Store {
    private let database: Database
    private let connection: Connection

    init(database: Database, connection: Connection) {
        self.database = database
        self.connection = connection
    }

    static func create(url string: String, dataType: String = "csv", name: String = "temp") async throws -> Store {
        guard let url = URL(string: string) else { throw "Invalid url" }
        let database = try Database(store: .inMemory)
        let connection = try database.connect()
        let (fileURL, _) = try await URLSession.shared.download(from: url)
        let function = switch dataType {
        case "json": "read_json_auto"
        case "parquet": "read_parquet"
        default: "read_csv"
        }
        try connection.execute("CREATE TABLE \(name) AS SELECT * FROM \(function)('\(fileURL.path)');")
        return Store(database: database, connection: connection)
    }

    func run(query: String) async throws -> DataFrame {
        let result = try connection.query(query)
        return DataFrame(columns: (0 ..< result.columnCount).map {
            result.column(at: $0).column
        })
    }
}

extension DuckDB.Column {
    var column: TabularData.AnyColumn {
        switch underlyingLogicalType.dataType {
        case .varchar, .uuid:
            TabularData.Column(self.cast(to: String.self)).eraseToAnyColumn()
        case .integer, .bigint, .uinteger, .ubigint:
            TabularData.Column(self.cast(to: Int.self)).eraseToAnyColumn()
        case .double, .float, .decimal:
            TabularData.Column(self.cast(to: Double.self)).eraseToAnyColumn()
        case .date:
            TabularData.Column(self.cast(to: Date.self)).eraseToAnyColumn()
        default:
            TabularData.Column(self).eraseToAnyColumn()
        }
    }
}

extension String: Error {}
