def read_html_file(file_path):
    """
    Reads an HTML file and returns its content as a string.

    Args:
        file_path (str): The path to the HTML file.

    Returns:
        str: The content of the HTML file.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        return content
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
    except IOError:
        print(f"Error: An IO error occurred while reading the file at {file_path}.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Example usage:
html_content = read_html_file('path/to/your/file.html')
if html_content:
    print(html_content)
