import psycopg2
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

def connect_db():
    """Establece una conexión con la base de datos PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname="etl",      
            user="postgres",   
            password="12345",   
            host="localhost",   
            port="5432"         
        )
        return conn
    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")
        return None

def extract_data(conn):
    """Extrae los datos necesarios desde la base de datos."""
    query = """
    SELECT 
    c."CustomerId", 
    c."FirstName" || ' ' || c."LastName" AS "CustomerName", 
    il."Quantity", 
    il."UnitPrice", 
    t."TrackId", 
    t."Name" AS "TrackName", 
    a."Title" AS "AlbumTitle", 
    g."Name" AS "GenreName" 
    FROM "InvoiceLine" il
    JOIN "Invoice" i ON il."InvoiceId" = i."InvoiceId"
    JOIN "Customer" c ON i."CustomerId" = c."CustomerId"
    JOIN "Track" t ON il."TrackId" = t."TrackId"
    JOIN "Album" a ON t."AlbumId" = a."AlbumId"
    JOIN "Genre" g ON t."GenreId" = g."GenreId";
    """
    return pd.read_sql_query(query, conn)

def transform_data(df):
    """Transforma los datos para el análisis."""
    
    df['TotalSpent'] = df['Quantity'] * df['UnitPrice']
    customer_segment = df.groupby('CustomerName').agg({
        'TotalSpent': 'sum'
    }).sort_values(by='TotalSpent', ascending=False).reset_index().head(10)  

    genre_sales = df.groupby('GenreName').agg({
        'TotalSpent': 'sum'
    }).sort_values(by='TotalSpent', ascending=False).reset_index()

    filtered_tracks = df[df['TrackName'] != 'Untitled']
    top_tracks = filtered_tracks.groupby('TrackName').agg({
        'Quantity': 'sum'
    }).sort_values(by='Quantity', ascending=False).reset_index().head(10)
    
    return customer_segment, genre_sales, top_tracks


def save_to_csv(customer_segment, genre_sales, top_tracks):
    """Guarda los resultados transformados en archivos CSV."""
    customer_segment.to_csv('customer_segment.csv', index=False)
    genre_sales.to_csv('genre_sales.csv', index=False)
    top_tracks.to_csv('top_tracks.csv', index=False)

def plot_and_save(data, filename, title, x_title, y_title, x_col, y_col):
    """Genera un gráfico de barras y lo guarda como un archivo HTML."""
    fig = go.Figure(data=[go.Bar(x=data[x_col], y=data[y_col])])
    fig.update_layout(
        title=title,
        xaxis_title=x_title,
        yaxis_title=y_title,
        xaxis_tickangle=-45
    )
    pio.write_html(fig, file=filename, auto_open=True)

def main():
    conn = connect_db()
    if conn:
        try:
            df = extract_data(conn)
            customer_segment, genre_sales, top_tracks = transform_data(df)
            
            save_to_csv(customer_segment, genre_sales, top_tracks)
            
            plot_and_save(customer_segment, 'customer_segment.html', 'Top 10 Clientes por Gastos', 
                          'Nombre del Cliente', 'Total Gastado', 'CustomerName', 'TotalSpent')
            
            plot_and_save(genre_sales, 'genre_sales.html', 'Ventas por Género Musical', 
                          'Género', 'Ventas Totales', 'GenreName', 'TotalSpent')
            
            plot_and_save(top_tracks, 'top_tracks.html', 'Top 10 Productos Más Vendidos', 
                          'Nombre de la Pista', 'Cantidad Vendida', 'TrackName', 'Quantity')
        except Exception as e:
            print(f"Error durante el procesamiento de datos: {e}")
        finally:
            conn.close()
    else:
        print("No se pudo establecer la conexión con la base de datos.")

if __name__ == "__main__":
    main()
