#!/usr/bin/env python3

import asyncio
import json
import re
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    LoggingLevel
)
import mcp.types as types

server = Server("latex-flashcard-mcp-server")

class AnkiConnector:
    """Interface for connecting to Anki via AnkiConnect addon."""
    
    def __init__(self, url: str = "http://localhost:8765", api_key: Optional[str] = None):
        self.url = url
        self.api_key = api_key
        self.session = requests.Session()
    
    def _make_request(self, action: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make a request to AnkiConnect API."""
        if params is None:
            params = {}
        
        payload = {
            "action": action,
            "version": 6,
            "params": params
        }
        
        if self.api_key:
            payload["key"] = self.api_key
        
        try:
            response = self.session.post(self.url, json=payload, timeout=10)
            response.raise_for_status()
            
            result = response.json()
            if result.get("error"):
                raise Exception(f"AnkiConnect error: {result['error']}")
            
            return result.get("result")
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to connect to Anki: {e}")
    
    def check_permission(self) -> Dict[str, Any]:
        """Check if AnkiConnect is available and get permission info."""
        return self._make_request("requestPermission")
    
    def get_deck_names(self) -> List[str]:
        """Get list of available deck names."""
        return self._make_request("deckNames")
    
    def get_model_names(self) -> List[str]:
        """Get list of available note type names."""
        return self._make_request("modelNames")
    
    def get_model_field_names(self, model_name: str) -> List[str]:
        """Get field names for a specific note type."""
        return self._make_request("modelFieldNames", {"modelName": model_name})
    
    def create_deck(self, deck_name: str) -> None:
        """Create a new deck if it doesn't exist."""
        self._make_request("createDeck", {"deck": deck_name})
    
    def add_note(self, deck_name: str, model_name: str, fields: Dict[str, str], 
                 tags: List[str] = None) -> Optional[int]:
        """Add a single note to Anki."""
        if tags is None:
            tags = []
        
        note = {
            "deckName": deck_name,
            "modelName": model_name,
            "fields": fields,
            "tags": tags
        }
        
        return self._make_request("addNote", {"note": note})
    
    def add_notes(self, notes: List[Dict[str, Any]]) -> List[Optional[int]]:
        """Add multiple notes to Anki."""
        formatted_notes = []
        for note in notes:
            formatted_notes.append({
                "deckName": note["deck_name"],
                "modelName": note["model_name"],
                "fields": note["fields"],
                "tags": note.get("tags", [])
            })
        
        return self._make_request("addNotes", {"notes": formatted_notes})

class FlashcardGenerator:
    """Generates LaTeX flashcards from text and diagrams."""
    
    @staticmethod
    def escape_latex(text: str) -> str:
        """Escape special LaTeX characters."""
        latex_special_chars = {
            '&': r'\&',
            '%': r'\%',
            '$': r'\$',
            '#': r'\#',
            '^': r'\textasciicircum{}',
            '_': r'\_',
            '{': r'\{',
            '}': r'\}',
            '~': r'\textasciitilde{}',
            '\\': r'\textbackslash{}'
        }
        
        for char, escaped in latex_special_chars.items():
            text = text.replace(char, escaped)
        return text
    
    @staticmethod
    def create_front_back_card(front: str, back: str) -> str:
        """Create a front-back flashcard in LaTeX format."""
        front_escaped = FlashcardGenerator.escape_latex(front)
        back_escaped = FlashcardGenerator.escape_latex(back)
        
        return f"""\\begin{{flashcard}}{{{front_escaped}}}
{back_escaped}
\\end{{flashcard}}"""
    
    @staticmethod
    def create_cloze_deletion_card(text: str, cloze_markers: List[str] = None) -> str:
        """Create a cloze deletion card in LaTeX format."""
        if cloze_markers is None:
            cloze_markers = ['{{', '}}']
        
        # Find text within cloze markers
        pattern = f"{re.escape(cloze_markers[0])}(.*?){re.escape(cloze_markers[1])}"
        matches = re.findall(pattern, text)
        
        if not matches:
            raise ValueError("No cloze deletions found in text")
        
        # Create the card with cloze deletions
        card_text = text
        for i, match in enumerate(matches, 1):
            replacement = f"\\\\cloze{{{match}}}"
            card_text = card_text.replace(f"{cloze_markers[0]}{match}{cloze_markers[1]}", replacement, 1)
        
        escaped_text = FlashcardGenerator.escape_latex(card_text)
        
        return f"""\\begin{{clozecard}}
{escaped_text}
\\end{{clozecard}}"""
    
    @staticmethod
    def parse_text_to_cards(text: str, card_type: str = "front-back") -> List[str]:
        """Parse text into multiple flashcards."""
        cards = []
        
        if card_type == "front-back":
            # Split by double newlines or specific separators
            sections = re.split(r'\n\s*\n|---', text.strip())
            
            for section in sections:
                section = section.strip()
                if not section:
                    continue
                    
                # Look for Q: A: pattern
                qa_match = re.search(r'Q:\s*(.*?)\s*A:\s*(.*)', section, re.DOTALL | re.IGNORECASE)
                if qa_match:
                    cards.append(FlashcardGenerator.create_front_back_card(
                        qa_match.group(1).strip(), 
                        qa_match.group(2).strip()
                    ))
                    continue
                
                # Look for question/answer separated by newline
                lines = section.split('\n')
                if len(lines) >= 2:
                    front = lines[0].strip()
                    back = '\n'.join(lines[1:]).strip()
                    cards.append(FlashcardGenerator.create_front_back_card(front, back))
        
        elif card_type == "cloze":
            # Split by double newlines for multiple cloze cards
            sections = re.split(r'\n\s*\n', text.strip())
            
            for section in sections:
                section = section.strip()
                if not section:
                    continue
                    
                try:
                    cards.append(FlashcardGenerator.create_cloze_deletion_card(section))
                except ValueError:
                    # If no cloze markers found, skip this section
                    continue
        
        return cards
    
    @staticmethod
    def process_diagram(diagram_text: str, diagram_type: str = "ascii") -> str:
        """Process diagrams and convert them to LaTeX format."""
        if diagram_type == "ascii":
            # Wrap ASCII diagrams in verbatim environment
            return f"\\begin{{verbatim}}\n{diagram_text}\n\\end{{verbatim}}"
        elif diagram_type == "tikz":
            # If it's already TikZ code, wrap it properly
            if "\\begin{tikzpicture}" not in diagram_text:
                return f"\\begin{{tikzpicture}}\n{diagram_text}\n\\end{{tikzpicture}}"
            return diagram_text
        elif diagram_type == "flowchart":
            # Convert simple flowchart notation to TikZ
            return FlashcardGenerator._convert_flowchart_to_tikz(diagram_text)
        else:
            # Default to verbatim for unknown types
            return f"\\begin{{verbatim}}\n{diagram_text}\n\\end{{verbatim}}"
    
    @staticmethod
    def _convert_flowchart_to_tikz(flowchart: str) -> str:
        """Convert simple flowchart notation to TikZ."""
        # This is a basic implementation - could be expanded
        lines = flowchart.strip().split('\n')
        tikz_code = "\\begin{tikzpicture}[node distance=2cm]\n"
        
        # Simple parsing for basic flowchart elements
        for i, line in enumerate(lines):
            line = line.strip()
            if '-->' in line:
                parts = line.split('-->')
                if len(parts) == 2:
                    from_node = parts[0].strip().replace(' ', '_')
                    to_node = parts[1].strip().replace(' ', '_')
                    tikz_code += f"\\draw[->] ({from_node}) -- ({to_node});\n"
            elif line:
                # Create a node
                node_name = line.replace(' ', '_')
                tikz_code += f"\\node ({node_name}) {{{line}}};\n"
        
        tikz_code += "\\end{tikzpicture}"
        return tikz_code
    
    @staticmethod
    def create_diagram_card(diagram: str, explanation: str, diagram_type: str = "ascii") -> str:
        """Create a flashcard with a diagram and explanation."""
        processed_diagram = FlashcardGenerator.process_diagram(diagram, diagram_type)
        escaped_explanation = FlashcardGenerator.escape_latex(explanation)
        
        return f"""\\begin{{flashcard}}{{Diagram}}
{processed_diagram}

\\vspace{{1em}}

{escaped_explanation}
\\end{{flashcard}}"""
    
    @staticmethod
    def create_latex_document(cards: List[str], title: str = "Flashcards") -> str:
        """Wrap flashcards in a complete LaTeX document."""
        escaped_title = FlashcardGenerator.escape_latex(title)
        
        header = f"""\\documentclass{{article}}
\\usepackage[utf8]{{inputenc}}
\\usepackage{{flashcards}}
\\usepackage{{amsmath}}
\\usepackage{{amsfonts}}
\\usepackage{{amssymb}}
\\usepackage{{tikz}}
\\usepackage{{verbatim}}
\\usetikzlibrary{{shapes,arrows,positioning}}

\\title{{{escaped_title}}}
\\author{{Generated by MCP Flashcard Server}}
\\date{{\\today}}

\\begin{{document}}

\\maketitle

"""
        
        footer = """
\\end{document}
"""
        
        cards_content = '\n\n'.join(cards)
        
        return header + cards_content + footer

class AnkiCardManager:
    """Manages conversion from generated flashcards to Anki cards."""
    
    def __init__(self, anki_connector: AnkiConnector):
        self.anki = anki_connector
    
    def get_default_model_for_card_type(self, card_type: str) -> str:
        """Get the appropriate Anki model for a flashcard type."""
        model_mapping = {
            "front-back": "Basic",
            "cloze": "Cloze",
            "diagram": "Basic"
        }
        return model_mapping.get(card_type, "Basic")
    
    def convert_to_anki_fields(self, card_content: str, card_type: str, model_name: str = None) -> Dict[str, str]:
        """Convert generated flashcard content to Anki field format."""
        if model_name is None:
            model_name = self.get_default_model_for_card_type(card_type)
        
        try:
            field_names = self.anki.get_model_field_names(model_name)
        except:
            # Fallback to basic fields if model not found
            field_names = ["Front", "Back"]
        
        if card_type == "front-back":
            return self._parse_front_back_card(card_content, field_names)
        elif card_type == "cloze":
            return self._parse_cloze_card(card_content, field_names)
        elif card_type == "diagram":
            return self._parse_diagram_card(card_content, field_names)
        else:
            # Default fallback
            return {field_names[0]: card_content}
    
    def _parse_front_back_card(self, content: str, field_names: List[str]) -> Dict[str, str]:
        """Parse front-back card content for Anki fields."""
        # Remove LaTeX flashcard environment
        content = re.sub(r'\\begin\{flashcard\}.*?\n', '', content)
        content = re.sub(r'\\end\{flashcard\}.*', '', content)
        
        # Try to split into front and back
        parts = content.strip().split('\n', 1)
        
        fields = {}
        if len(parts) >= 2:
            fields[field_names[0]] = parts[0].strip()
            fields[field_names[1]] = parts[1].strip()
        else:
            fields[field_names[0]] = content.strip()
            if len(field_names) > 1:
                fields[field_names[1]] = ""
        
        return fields
    
    def _parse_cloze_card(self, content: str, field_names: List[str]) -> Dict[str, str]:
        """Parse cloze card content for Anki fields."""
        # Remove LaTeX cloze environment
        content = re.sub(r'\\begin\{clozecard\}.*?\n', '', content)
        content = re.sub(r'\\end\{clozecard\}.*', '', content)
        
        # Convert LaTeX cloze format to Anki cloze format
        content = re.sub(r'\\\\cloze\{([^}]+)\}', r'{{c1::\1}}', content)
        
        fields = {field_names[0]: content.strip()}
        return fields
    
    def _parse_diagram_card(self, content: str, field_names: List[str]) -> Dict[str, str]:
        """Parse diagram card content for Anki fields."""
        # Remove LaTeX flashcard environment
        content = re.sub(r'\\begin\{flashcard\}.*?\n', '', content)
        content = re.sub(r'\\end\{flashcard\}.*', '', content)
        
        # Split diagram and explanation
        parts = content.split('\\vspace{1em}')
        
        fields = {}
        if len(parts) >= 2:
            fields[field_names[0]] = parts[0].strip()
            if len(field_names) > 1:
                fields[field_names[1]] = parts[1].strip()
        else:
            fields[field_names[0]] = content.strip()
            if len(field_names) > 1:
                fields[field_names[1]] = ""
        
        return fields
    
    def upload_cards_to_anki(self, cards_data: List[Dict[str, Any]], deck_name: str) -> Dict[str, Any]:
        """Upload multiple cards to Anki."""
        try:
            # Check connection and create deck if needed
            permission_info = self.anki.check_permission()
            self.anki.create_deck(deck_name)
            
            # Convert and upload cards
            anki_notes = []
            for card_data in cards_data:
                card_type = card_data.get("card_type", "front-back")
                model_name = card_data.get("model_name", self.get_default_model_for_card_type(card_type))
                
                fields = self.convert_to_anki_fields(
                    card_data["content"], 
                    card_type, 
                    model_name
                )
                
                anki_notes.append({
                    "deck_name": deck_name,
                    "model_name": model_name,
                    "fields": fields,
                    "tags": card_data.get("tags", ["mcp-generated"])
                })
            
            note_ids = self.anki.add_notes(anki_notes)
            
            success_count = sum(1 for note_id in note_ids if note_id is not None)
            failed_count = len(note_ids) - success_count
            
            return {
                "success": True,
                "total_cards": len(cards_data),
                "successful_uploads": success_count,
                "failed_uploads": failed_count,
                "deck_name": deck_name,
                "note_ids": note_ids
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "total_cards": len(cards_data)
            }

class HTMLCardRenderer:
    """Renders flashcards as HTML for preview purposes."""
    
    @staticmethod
    def get_base_css() -> str:
        """Get base CSS styles for flashcard rendering."""
        return """
        <style>
        .flashcard-container {
            max-width: 600px;
            margin: 20px auto;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        .flashcard {
            border: 2px solid #e0e0e0;
            border-radius: 12px;
            margin: 16px 0;
            background: white;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        .card-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 12px 20px;
            font-weight: 600;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .card-front, .card-back {
            padding: 24px;
            min-height: 60px;
            display: flex;
            align-items: center;
            justify-content: center;
            text-align: center;
        }
        .card-front {
            background: #f8f9fa;
            border-bottom: 1px solid #e0e0e0;
            font-size: 18px;
            font-weight: 500;
            color: #2c3e50;
        }
        .card-back {
            background: white;
            font-size: 16px;
            line-height: 1.6;
            color: #34495e;
        }
        .cloze-card {
            padding: 24px;
            background: white;
            font-size: 16px;
            line-height: 1.6;
            color: #2c3e50;
        }
        .cloze-blank {
            background: #3498db;
            color: white;
            padding: 2px 8px;
            border-radius: 4px;
            font-weight: 500;
        }
        .diagram-container {
            background: #f8f9fa;
            padding: 20px;
            margin: 16px 0;
            border-radius: 8px;
            border: 1px solid #dee2e6;
        }
        .diagram-code {
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            background: #2c3e50;
            color: #ecf0f1;
            padding: 16px;
            border-radius: 6px;
            white-space: pre-wrap;
            overflow-x: auto;
            font-size: 14px;
            line-height: 1.4;
        }
        .math {
            font-style: italic;
            color: #8e44ad;
        }
        .tag {
            display: inline-block;
            background: #e8f4f8;
            color: #2980b9;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 12px;
            margin: 2px;
            font-weight: 500;
        }
        .tags-container {
            padding: 12px 20px;
            background: #f8f9fa;
            border-top: 1px solid #e0e0e0;
        }
        .preview-header {
            text-align: center;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            margin-bottom: 0;
        }
        .preview-header h1 {
            margin: 0;
            font-size: 24px;
            font-weight: 600;
        }
        .preview-header p {
            margin: 8px 0 0 0;
            opacity: 0.9;
            font-size: 14px;
        }
        </style>
        """
    
    @staticmethod
    def get_mathjax_script() -> str:
        """Get MathJax configuration script for LaTeX rendering."""
        return """
        <script src="https://polyfill.io/v3/polyfill.min.js?features=es6"></script>
        <script id="MathJax-script" async src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>
        <script>
        window.MathJax = {
            tex: {
                inlineMath: [['$', '$'], ['\\\\(', '\\\\)']],
                displayMath: [['$$', '$$'], ['\\\\[', '\\\\]']],
                processEscapes: true,
                processEnvironments: true
            },
            options: {
                ignoreHtmlClass: 'tex2jax_ignore',
                processHtmlClass: 'tex2jax_process'
            }
        };
        </script>
        """
    
    @staticmethod
    def convert_latex_to_mathjax(text: str) -> str:
        """Convert LaTeX syntax to MathJax-compatible format."""
        # Convert display math
        text = re.sub(r'\\begin\{equation\}(.*?)\\end\{equation\}', r'$$\1$$', text, flags=re.DOTALL)
        text = re.sub(r'\\begin\{align\}(.*?)\\end\{align\}', r'$$\\begin{align}\1\\end{align}$$', text, flags=re.DOTALL)
        
        # Convert inline math (be careful not to double-convert)
        text = re.sub(r'(?<!\\)\$(?!\$)(.*?)(?<!\\)\$(?!\$)', r'$\1$', text)
        
        # Convert common LaTeX commands
        text = text.replace('\\LaTeX', '$\\LaTeX$')
        text = text.replace('\\TeX', '$\\TeX$')
        
        # Clean up escaped characters for HTML
        text = text.replace('\\&', '&')
        text = text.replace('\\%', '%')
        text = text.replace('\\$', '$')
        text = text.replace('\\#', '#')
        text = text.replace('\\_', '_')
        text = text.replace('\\{', '{')
        text = text.replace('\\}', '}')
        
        return text
    
    @staticmethod
    def render_front_back_card(front: str, back: str, tags: List[str] = None) -> str:
        """Render a front-back card as HTML."""
        if tags is None:
            tags = []
        
        front_html = HTMLCardRenderer.convert_latex_to_mathjax(front)
        back_html = HTMLCardRenderer.convert_latex_to_mathjax(back)
        
        tags_html = ""
        if tags:
            tags_html = f"""
            <div class="tags-container">
                {''.join(f'<span class="tag">{tag}</span>' for tag in tags)}
            </div>
            """
        
        return f"""
        <div class="flashcard">
            <div class="card-header">Front-Back Card</div>
            <div class="card-front">{front_html}</div>
            <div class="card-back">{back_html}</div>
            {tags_html}
        </div>
        """
    
    @staticmethod
    def render_cloze_card(text: str, tags: List[str] = None) -> str:
        """Render a cloze deletion card as HTML."""
        if tags is None:
            tags = []
        
        # Convert cloze deletions to HTML
        cloze_html = re.sub(r'\{\{c1::(.*?)\}\}', r'<span class="cloze-blank">\1</span>', text)
        cloze_html = HTMLCardRenderer.convert_latex_to_mathjax(cloze_html)
        
        tags_html = ""
        if tags:
            tags_html = f"""
            <div class="tags-container">
                {''.join(f'<span class="tag">{tag}</span>' for tag in tags)}
            </div>
            """
        
        return f"""
        <div class="flashcard">
            <div class="card-header">Cloze Deletion Card</div>
            <div class="cloze-card">{cloze_html}</div>
            {tags_html}
        </div>
        """
    
    @staticmethod
    def render_diagram_card(diagram: str, explanation: str, diagram_type: str = "ascii", tags: List[str] = None) -> str:
        """Render a diagram card as HTML."""
        if tags is None:
            tags = []
        
        # Process diagram based on type
        if diagram_type == "ascii":
            diagram_html = f'<div class="diagram-code">{diagram}</div>'
        elif diagram_type == "tikz":
            # For TikZ, show the code for now (could be enhanced with TikZ to SVG conversion)
            diagram_html = f'<div class="diagram-code">{diagram}</div><p><em>TikZ Diagram Code</em></p>'
        else:
            diagram_html = f'<div class="diagram-code">{diagram}</div>'
        
        explanation_html = HTMLCardRenderer.convert_latex_to_mathjax(explanation)
        
        tags_html = ""
        if tags:
            tags_html = f"""
            <div class="tags-container">
                {''.join(f'<span class="tag">{tag}</span>' for tag in tags)}
            </div>
            """
        
        return f"""
        <div class="flashcard">
            <div class="card-header">Diagram Card</div>
            <div class="card-front">
                <div class="diagram-container">
                    {diagram_html}
                </div>
            </div>
            <div class="card-back">{explanation_html}</div>
            {tags_html}
        </div>
        """
    
    @staticmethod
    def render_cards_preview(cards_data: List[Dict[str, Any]], title: str = "Flashcard Preview") -> str:
        """Render multiple cards as a complete HTML document."""
        cards_html = []
        
        for card_data in cards_data:
            card_type = card_data.get("card_type", "front-back")
            tags = card_data.get("tags", [])
            
            if card_type == "front-back":
                # Extract front and back from content
                content = card_data["content"]
                # Simple parsing - could be enhanced
                parts = content.replace("\\begin{flashcard}", "").replace("\\end{flashcard}", "").strip().split('\n', 1)
                front = parts[0].strip() if parts else "No content"
                back = parts[1].strip() if len(parts) > 1 else ""
                
                cards_html.append(HTMLCardRenderer.render_front_back_card(front, back, tags))
                
            elif card_type == "cloze":
                content = card_data["content"]
                # Clean up cloze content
                content = content.replace("\\begin{clozecard}", "").replace("\\end{clozecard}", "").strip()
                # Convert LaTeX cloze to standard format
                content = re.sub(r'\\\\cloze\{([^}]+)\}', r'{{c1::\1}}', content)
                
                cards_html.append(HTMLCardRenderer.render_cloze_card(content, tags))
                
            elif card_type == "diagram":
                # Extract diagram and explanation
                content = card_data["content"]
                content = content.replace("\\begin{flashcard}{Diagram}", "").replace("\\end{flashcard}", "").strip()
                
                # Split by vspace
                parts = content.split('\\vspace{1em}')
                diagram = parts[0].strip() if parts else "No diagram"
                explanation = parts[1].strip() if len(parts) > 1 else "No explanation"
                
                # Detect diagram type
                diagram_type = "tikz" if "tikzpicture" in diagram else "ascii"
                
                cards_html.append(HTMLCardRenderer.render_diagram_card(diagram, explanation, diagram_type, tags))
        
        full_html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{title}</title>
            {HTMLCardRenderer.get_base_css()}
            {HTMLCardRenderer.get_mathjax_script()}
        </head>
        <body>
            <div class="preview-header">
                <h1>{title}</h1>
                <p>{len(cards_data)} card{"s" if len(cards_data) != 1 else ""} generated</p>
            </div>
            <div class="flashcard-container">
                {''.join(cards_html)}
            </div>
        </body>
        </html>
        """
        
        return full_html

@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="create_flashcards",
            description="Convert text or diagrams into LaTeX flashcards for Anki",
            inputSchema={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "Text or diagram content to convert to flashcards"
                    },
                    "card_type": {
                        "type": "string",
                        "enum": ["front-back", "cloze"],
                        "description": "Type of flashcard to create",
                        "default": "front-back"
                    },
                    "title": {
                        "type": "string",
                        "description": "Title for the flashcard deck",
                        "default": "Flashcards"
                    },
                    "full_document": {
                        "type": "boolean",
                        "description": "Whether to return a complete LaTeX document or just the cards",
                        "default": True
                    }
                },
                "required": ["content"]
            }
        ),
        Tool(
            name="create_diagram_card",
            description="Create a flashcard with a diagram and explanation",
            inputSchema={
                "type": "object",
                "properties": {
                    "diagram": {
                        "type": "string",
                        "description": "The diagram content (ASCII art, TikZ code, or flowchart notation)"
                    },
                    "explanation": {
                        "type": "string",
                        "description": "Explanation or description of the diagram"
                    },
                    "diagram_type": {
                        "type": "string",
                        "enum": ["ascii", "tikz", "flowchart"],
                        "description": "Type of diagram",
                        "default": "ascii"
                    }
                },
                "required": ["diagram", "explanation"]
            }
        ),
        Tool(
            name="create_single_card",
            description="Create a single LaTeX flashcard",
            inputSchema={
                "type": "object",
                "properties": {
                    "front": {
                        "type": "string",
                        "description": "Front of the card (for front-back cards)"
                    },
                    "back": {
                        "type": "string",
                        "description": "Back of the card (for front-back cards)"
                    },
                    "cloze_text": {
                        "type": "string",
                        "description": "Text with cloze deletions marked by {{ }} (for cloze cards)"
                    },
                    "card_type": {
                        "type": "string",
                        "enum": ["front-back", "cloze"],
                        "description": "Type of flashcard to create",
                        "default": "front-back"
                    }
                },
                "anyOf": [
                    {"required": ["front", "back"]},
                    {"required": ["cloze_text"]}
                ]
            }
        ),
        Tool(
            name="upload_to_anki",
            description="Upload generated flashcards directly to Anki",
            inputSchema={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "Text content to convert and upload to Anki"
                    },
                    "card_type": {
                        "type": "string",
                        "enum": ["front-back", "cloze"],
                        "description": "Type of flashcard to create",
                        "default": "front-back"
                    },
                    "deck_name": {
                        "type": "string",
                        "description": "Name of the Anki deck to upload to",
                        "default": "MCP Generated Cards"
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tags to add to the cards",
                        "default": ["mcp-generated"]
                    },
                    "anki_api_key": {
                        "type": "string",
                        "description": "Optional AnkiConnect API key if authentication is enabled"
                    }
                },
                "required": ["content"]
            }
        ),
        Tool(
            name="check_anki_connection",
            description="Check connection to Anki and get available decks/models",
            inputSchema={
                "type": "object",
                "properties": {
                    "anki_api_key": {
                        "type": "string",
                        "description": "Optional AnkiConnect API key if authentication is enabled"
                    }
                }
            }
        ),
        Tool(
            name="preview_cards",
            description="Generate HTML preview of flashcards for easy visualization",
            inputSchema={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "Text content to convert to flashcards and preview"
                    },
                    "card_type": {
                        "type": "string",
                        "enum": ["front-back", "cloze"],
                        "description": "Type of flashcard to create",
                        "default": "front-back"
                    },
                    "title": {
                        "type": "string",
                        "description": "Title for the preview document",
                        "default": "Flashcard Preview"
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tags to display on the cards",
                        "default": []
                    }
                },
                "required": ["content"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """Handle tool calls."""
    if name == "create_flashcards":
        content = arguments.get("content", "")
        card_type = arguments.get("card_type", "front-back")
        title = arguments.get("title", "Flashcards")
        full_document = arguments.get("full_document", True)
        
        try:
            cards = FlashcardGenerator.parse_text_to_cards(content, card_type)
            
            if not cards:
                return [types.TextContent(
                    type="text", 
                    text="No flashcards could be generated from the provided content. Please check the format."
                )]
            
            if full_document:
                result = FlashcardGenerator.create_latex_document(cards, title)
            else:
                result = '\n\n'.join(cards)
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error generating flashcards: {str(e)}")]
    
    elif name == "create_diagram_card":
        diagram = arguments.get("diagram", "")
        explanation = arguments.get("explanation", "")
        diagram_type = arguments.get("diagram_type", "ascii")
        
        if not diagram or not explanation:
            return [types.TextContent(type="text", text="Both diagram and explanation are required")]
        
        try:
            result = FlashcardGenerator.create_diagram_card(diagram, explanation, diagram_type)
            return [types.TextContent(type="text", text=result)]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error creating diagram card: {str(e)}")]
    
    elif name == "create_single_card":
        card_type = arguments.get("card_type", "front-back")
        
        try:
            if card_type == "front-back":
                front = arguments.get("front", "")
                back = arguments.get("back", "")
                if not front or not back:
                    return [types.TextContent(type="text", text="Both front and back are required for front-back cards")]
                
                result = FlashcardGenerator.create_front_back_card(front, back)
                
            elif card_type == "cloze":
                cloze_text = arguments.get("cloze_text", "")
                if not cloze_text:
                    return [types.TextContent(type="text", text="Cloze text is required for cloze cards")]
                
                result = FlashcardGenerator.create_cloze_deletion_card(cloze_text)
            
            return [types.TextContent(type="text", text=result)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error creating flashcard: {str(e)}")]
    
    elif name == "upload_to_anki":
        content = arguments.get("content", "")
        card_type = arguments.get("card_type", "front-back")
        deck_name = arguments.get("deck_name", "MCP Generated Cards")
        tags = arguments.get("tags", ["mcp-generated"])
        anki_api_key = arguments.get("anki_api_key")
        
        if not content:
            return [types.TextContent(type="text", text="Content is required for uploading to Anki")]
        
        try:
            # Generate flashcards from content
            cards = FlashcardGenerator.parse_text_to_cards(content, card_type)
            
            if not cards:
                return [types.TextContent(type="text", text="No flashcards could be generated from the provided content")]
            
            # Initialize Anki connection
            anki_connector = AnkiConnector(api_key=anki_api_key)
            card_manager = AnkiCardManager(anki_connector)
            
            # Prepare cards data for upload
            cards_data = []
            for card_content in cards:
                cards_data.append({
                    "content": card_content,
                    "card_type": card_type,
                    "tags": tags
                })
            
            # Upload to Anki
            result = card_manager.upload_cards_to_anki(cards_data, deck_name)
            
            if result["success"]:
                response = f"Successfully uploaded {result['successful_uploads']} cards to Anki deck '{result['deck_name']}'"
                if result["failed_uploads"] > 0:
                    response += f" ({result['failed_uploads']} failed)"
                return [types.TextContent(type="text", text=response)]
            else:
                return [types.TextContent(type="text", text=f"Failed to upload cards to Anki: {result['error']}")]
                
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error uploading to Anki: {str(e)}")]
    
    elif name == "check_anki_connection":
        anki_api_key = arguments.get("anki_api_key")
        
        try:
            anki_connector = AnkiConnector(api_key=anki_api_key)
            
            # Check connection and permissions
            permission_info = anki_connector.check_permission()
            
            # Get available decks and models
            decks = anki_connector.get_deck_names()
            models = anki_connector.get_model_names()
            
            response = f"""Anki Connection Status: ✓ Connected
            
Permission: {permission_info.get('permission', 'unknown')}
API Key Required: {permission_info.get('requireApiKey', False)}
Version: {permission_info.get('version', 'unknown')}

Available Decks ({len(decks)}):
{', '.join(decks[:10])}{'...' if len(decks) > 10 else ''}

Available Note Types ({len(models)}):
{', '.join(models[:10])}{'...' if len(models) > 10 else ''}
"""
            
            return [types.TextContent(type="text", text=response)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Failed to connect to Anki: {str(e)}\n\nMake sure Anki is running and AnkiConnect addon is installed.")]
    
    elif name == "preview_cards":
        content = arguments.get("content", "")
        card_type = arguments.get("card_type", "front-back")
        title = arguments.get("title", "Flashcard Preview")
        tags = arguments.get("tags", [])
        
        if not content:
            return [types.TextContent(type="text", text="Content is required for previewing cards")]
        
        try:
            # Generate flashcards from content
            cards = FlashcardGenerator.parse_text_to_cards(content, card_type)
            
            if not cards:
                return [types.TextContent(type="text", text="No flashcards could be generated from the provided content")]
            
            # Prepare cards data for rendering
            cards_data = []
            for card_content in cards:
                cards_data.append({
                    "content": card_content,
                    "card_type": card_type,
                    "tags": tags
                })
            
            # Generate HTML preview
            html_preview = HTMLCardRenderer.render_cards_preview(cards_data, title)
            
            return [types.TextContent(type="text", text=html_preview)]
            
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error generating preview: {str(e)}")]
    
    else:
        raise ValueError(f"Unknown tool: {name}")

async def main():
    from mcp.server.stdio import stdio_server
    
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream, 
            write_stream, 
            InitializationOptions(
                server_name="latex-flashcard-mcp-server",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={}
                )
            )
        )

if __name__ == "__main__":
    asyncio.run(main())