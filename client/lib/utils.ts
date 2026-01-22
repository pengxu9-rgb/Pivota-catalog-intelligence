export function getAdCopySubjectPreview(markdown: string) {
  try {
    const firstLine = markdown.split("\n")[0] || "";
    return firstLine
      .replace(/\*\*Subject:\*\*\s*/i, "")
      .replace(/\*\*Subject:\*\*/i, "")
      .trim() || "View Copy";
  } catch {
    return "View Copy";
  }
}

