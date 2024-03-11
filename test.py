def read_file_content(file_path):
    """
    Reads the content of a file and returns it as a string.
    
    Parameters:
    - file_path: The path to the file.
    
    Returns:
    - A string containing the content of the file.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        return "The file was not found."
    except Exception as e:
        return f"An error occurred: {e}"

# Example usage:
# file_content = read_file_content("path/to/your/file.txt")
# print(file_content)
