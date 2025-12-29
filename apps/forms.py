from django import forms
class ContactForm(forms.Form):
    name = forms.CharField(max_length=100, widget=forms.TextInput(attrs={"placeholder": "Your Name"}))
    surname = forms.CharField(max_length=100, widget=forms.TextInput(attrs={"placeholder": "Your Surname"}))
    email = forms.EmailField(
        widget=forms.TextInput(attrs={"placeholder": "Your e-mail"})
    )
    # subject = forms.CharField(widget=forms.TextInput(attrs={"placeholder": "Subject"})) # Subject is generated automatically
    message = forms.CharField(
        widget=forms.Textarea(attrs={"placeholder": "Your message"})
    )