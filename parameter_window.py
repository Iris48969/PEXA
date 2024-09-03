import tkinter as tk
from tkinter import ttk

def open_parameter_window():
    # This list will hold the parameters to return
    parameters = []

    def save_default_parameters():
        # Save the default values
        ratio_upper = 5
        ratio_lower = 1
        multiplier = 5
        sensitivity = 0.005
        contamination = 0.003
        sa4_code = 213
        
        save_parameters(ratio_upper, ratio_lower, multiplier, sensitivity, contamination, sa4_code)

    def save_entered_parameters():
        # Get values from entry fields, use entered values or defaults if empty
        ratio_upper = float(entry_param1.get()) if entry_param1.get() else 5
        ratio_lower = float(entry_param2.get()) if entry_param2.get() else 1
        multiplier = float(entry_param3.get()) if entry_param3.get() else 5
        sensitivity = float(entry_param4.get()) if entry_param4.get() else 0.005
        contamination = float(entry_param5.get()) if entry_param5.get() else 0.003
        sa4_code = int(entry_param6.get()) if entry_param6.get() else 213
        
        save_parameters(ratio_upper, ratio_lower, multiplier, sensitivity, contamination, sa4_code)

    def save_parameters(ratio_upper, ratio_lower, multiplier, sensitivity, contamination, sa4_code):
        with open('parameters.txt', 'w') as f:
            f.write(f"ratio_upper: {ratio_upper}\n")
            f.write(f"ratio_lower: {ratio_lower}\n")
            f.write(f"multiplier: {multiplier}\n")
            f.write(f"sensitivity: {sensitivity}\n")
            f.write(f"contamination: {contamination}\n")
            f.write(f"sa4_code: {sa4_code}\n")

        # Save parameters to the list
        parameters.extend([ratio_upper, ratio_lower, multiplier, sensitivity, contamination, sa4_code])
        
        print(f"ratio_upper set to: {ratio_upper}")
        print(f"ratio_lower set to: {ratio_lower}")
        print(f"multiplier set to: {multiplier}")
        print(f"sensitivity set to: {sensitivity}")
        print(f"contamination set to: {contamination}")
        print(f"sa4_code set to: {sa4_code}")
        window.destroy()  # Close the window after saving

    # Create the main window
    window = tk.Tk()
    window.title("Modify Parameters")

    # Create sections (labels and entry fields) for parameters
    ttk.Label(window, text="Ratio Upper:").grid(row=0, column=0, padx=10, pady=10)
    entry_param1 = ttk.Entry(window)
    entry_param1.grid(row=0, column=1, padx=10, pady=10)
    entry_param1.insert(0, "5")  # Set default value

    ttk.Label(window, text="Ratio Lower:").grid(row=1, column=0, padx=10, pady=10)
    entry_param2 = ttk.Entry(window)
    entry_param2.grid(row=1, column=1, padx=10, pady=10)
    entry_param2.insert(0, "1")  # Set default value

    ttk.Label(window, text="IQR Multiplier for spike detection:").grid(row=2, column=0, padx=10, pady=10)
    entry_param3 = ttk.Entry(window)
    entry_param3.grid(row=2, column=1, padx=10, pady=10)
    entry_param3.insert(0, "5")  # Set default value

    ttk.Label(window, text="Sensitivity (% change Below this value == no change)").grid(row=3, column=0, padx=10, pady=10)
    entry_param4 = ttk.Entry(window)
    entry_param4.grid(row=3, column=1, padx=10, pady=10)
    entry_param4.insert(0, "0.005")  # Set default value

    ttk.Label(window, text="Contamination:").grid(row=4, column=0, padx=10, pady=10)
    entry_param5 = ttk.Entry(window)
    entry_param5.grid(row=4, column=1, padx=10, pady=10)
    entry_param5.insert(0, "0.003")  # Set default value

    ttk.Label(window, text="SA4 Code:").grid(row=5, column=0, padx=10, pady=10)
    entry_param6 = ttk.Entry(window)
    entry_param6.grid(row=5, column=1, padx=10, pady=10)
    entry_param6.insert(0, "213")  # Set default value

    # Add a button to save default parameters
    default_button = ttk.Button(window, text="Use Default Values", command=save_default_parameters)
    default_button.grid(row=6, column=0, pady=10, padx=10)

    # Add a button to save entered parameters
    save_button = ttk.Button(window, text="Use Entered Values", command=save_entered_parameters)
    save_button.grid(row=6, column=1, pady=10, padx=10)

    # Start the GUI event loop
    window.mainloop()

    # Return the parameters after the window is closed
    return parameters
