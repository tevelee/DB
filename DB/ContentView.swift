import SwiftUI
import TabularData

struct ContentView: View {
    @State private var url: String = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+pl_name+,+disc_year+from+pscomppars&format=csv"
    @State private var query: String = """
    SELECT disc_year, count(disc_year) AS Count
    FROM source
    GROUP BY disc_year
    ORDER BY disc_year
    """
    @State private var data = DataFrame()
    @State private var showFileBrowser = false
    @State private var error: String?

    var body: some View {
        Form {
            Section {
                HStack {
                    TextField("Source", text: $url)
                    Button("Browse") {
                        showFileBrowser = true
                    }
                    .fileImporter(
                        isPresented: $showFileBrowser,
                        allowedContentTypes: [.fileURL, .json, .commaSeparatedText, .tabSeparatedText],
                        allowsMultipleSelection: false
                    ) { results in
                        switch results {
                        case .success(let fileURLs):
                            if let url = fileURLs.first {
                                self.url = url.relativeString
                            }
                        case .failure(let error):
                            self.error = error.localizedDescription
                        }
                    }
                }
                TextEditor(text: $query)
                    .frame(height: 50)
                Button {
                    Task {
                        await run()
                    }
                } label: {
                    Label("Run", systemImage: "play")
                }
            }

            Section {
                if #available(macOS 14.4, iOS 17.4, *) {
                    Table(data.rows) {
                        TableColumnForEach(data.columns, id: \.name) { column in
                            TableColumn(column.name) { row in
                                Text(String(describing: row[column.name]))
                            }
                        }
                    }
                } else if let first = data.columns.first {
                    if let second = data.columns.dropFirst().first {
                        Table(data.rows) {
                            TableColumn(first.name) {
                                Text(String(describing: $0[0]!))
                            }
                            TableColumn(second.name) {
                                Text(String(describing: $0[1]!))
                            }
                        }
                    } else {
                        Table(data.rows) {
                            TableColumn(first.name) {
                                Text(String(describing: $0[0]!))
                            }
                        }
                    }
                } else {
                    Color.clear
                }
            }
        }
        .padding()
        .task {
            await run()
        }
        .alert(item: $error) { error in
            Alert(title: Text(error))
        }
    }

    private func run() async {
        do {
            let store = try await Store.create(
                url: url,
                dataType: URL(string: url)?.pathExtension ?? "csv",
                name: "source"
            )
            data = try await store.run(query: query)
        } catch {
            self.error = error.localizedDescription
            data = .init()
        }
    }
}

#Preview {
    ContentView()
}

extension DataFrame.Rows: RandomAccessCollection {}
extension DataFrame.Row: Identifiable {
    public var id: Int { index }
}
extension String: Identifiable {
    public var id: String { self }
}
