import tkinter as tk
from tkinter import ttk

class ToolTip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tooltip_window = None
        self.widget.bind("<Enter>", self.show_tooltip)
        self.widget.bind("<Leave>", self.hide_tooltip)

    def show_tooltip(self, event):
        if self.tooltip_window or not self.text:
            return
        x, y, _, _ = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 20
        y += self.widget.winfo_rooty() + 20
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left',
                         background="lightyellow", relief='solid', borderwidth=1,
                         font=("Arial", 10, "normal"))
        label.pack(ipadx=5, ipady=5)

    def hide_tooltip(self, event):
        if self.tooltip_window:
            self.tooltip_window.destroy()
            self.tooltip_window = None

def open_parameter_window():
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
        
        window.destroy()  # Close the window after saving

    # Create the main window
    window = tk.Tk()
    window.title("Modify Parameters")

    # General style settings
    style = ttk.Style()
    style.configure("TLabel", font=("Arial", 11))
    style.configure("TEntry", font=("Arial", 11))

    # Function to create labels, entry fields, and tooltips
    def create_param_row(text, row, default_value, tooltip_text):
        label = ttk.Label(window, text=text)
        label.grid(row=row, column=0, padx=(10, 2), pady=10, sticky='w')
        entry = ttk.Entry(window)
        entry.grid(row=row, column=1, padx=(2, 10), pady=10)
        entry.insert(0, default_value)
        help_icon = tk.Label(window, text="？", foreground="blue", cursor="hand2", font=("Arial", 12, "bold"))
        help_icon.grid(row=row, column=2, padx=(0, 10), pady=10, sticky='e')
        ToolTip(help_icon, tooltip_text)
        return entry

    # Creating rows with parameters
    entry_param1 = create_param_row("Ratio Upper:", 0, "5", """Household Size Check: This sets the upper bound for the household size ratio.
    If the ratio is above this upper bound it will be treated as outlier.""")
    entry_param2 = create_param_row("Ratio Lower:", 1, "1", """Household Size Check: This sets the lower bound for the household size ratio.
    If the ratio is below this lower bound it will be treated as outlier.""")
    entry_param3 = create_param_row("""IQR Multiplier for spike detection:", 2, "5", "Outlier Detection: This multiplier is used to detect spikes in the data. 
    For example, the upper bound is Q3 + multiplier * IQR and if the growth of population is higher than upper bound or below the lower bound this region will be flagged""")
    entry_param4 = create_param_row("Sensitivity (% change):", 3, "0.005", """Population Check: This sets the sensitivity threshold for detecting significant population changes.
    When the absolute of population growth is lower than this sensitivity level, it will be treated as 'No change'.""")
    entry_param5 = create_param_row("Contamination:", 4, "0.003", """Anomaly Detection: Controls the contamination level used in the Isolation Forest algorithm.""")
    entry_param6 = create_param_row("SA4 Code:", 5, "213", "Geographical Area: This code represents the specific region being analyzed.")

    # Buttons to save parameters
    default_button = ttk.Button(window, text="Use Default Values", command=save_default_parameters)
    default_button.grid(row=6, column=0, pady=10, padx=10)
    save_button = ttk.Button(window, text="Use Entered Values", command=save_entered_parameters)
    save_button.grid(row=6, column=1, pady=10, padx=10)

    # Start the GUI event loop
    window.mainloop()

    # Return the parameters after the window is closed
    return parameters
