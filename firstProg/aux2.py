def sort_list_example(input_list):
    """
    Sorts a list in ascending order and returns the sorted list.

    Args:
      input_list: The list to be sorted.

    Returns:
      A new list containing the elements of the input list, sorted in ascending order.
    """
    # Create a copy to avoid modifying the original list
    my_list = input_list[:]  # Important: Create a copy!

    # There are two main ways to sort a list in Python:
    # 1. Using the sorted() function (returns a new sorted list)
    # 2. Using the list.sort() method (sorts the list in place)

    # Using sorted():
    sorted_list = sorted(my_list)  # Does not modify my_list
    print("Sorted using sorted():", sorted_list)

    # Using list.sort():
    my_list.sort()  # Sorts the list in place (modifies my_list)
    print("Sorted using sort():", my_list) # prints the sorted list

    return sorted_list # returning sorted list, just to show usage

def main():
    """
    Main function to demonstrate the usage of the sort_list_example function.
    """
    unsorted_list = [5, 2, 8, 1, 9, 3, 7, 4, 6]
    print("Original list:", unsorted_list)

    sorted_list = sort_list_example(unsorted_list) # calling the function and storing the sorted list

    print("Original list after calling sort_list_example:", unsorted_list)  # Unchanged, because of the copy

    print("Returned sorted list:", sorted_list) # printing the returned sorted list

    # Example with strings
    string_list = ["apple", "banana", "lololol", "date"]
    print("\nOriginal string list:", string_list)
    sorted_string_list = sorted(string_list)
    print("Sorted string list:", sorted_string_list) # prints sorted string list
    print("Original string list after sorting:", string_list) # prints original string list


if __name__ == "__main__":
    main()
