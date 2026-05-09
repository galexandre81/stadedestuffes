// Lightweight tweaks panel — only theme switch is wired in this redesign.
// The full host protocol from the prototype isn't needed for the live site;
// kept minimal so app.jsx renders without errors.

function useTweaks(defaults) {
  const [values, setValues] = React.useState(() => {
    try {
      const stored = localStorage.getItem('stt.tweaks');
      if (stored) return { ...defaults, ...JSON.parse(stored) };
    } catch (e) { /* ignore */ }
    return defaults;
  });
  const setTweak = React.useCallback((keyOrEdits, val) => {
    const edits = typeof keyOrEdits === 'object' && keyOrEdits !== null ? keyOrEdits : { [keyOrEdits]: val };
    setValues((prev) => {
      const next = { ...prev, ...edits };
      try { localStorage.setItem('stt.tweaks', JSON.stringify(next)); } catch (e) { /* ignore */ }
      return next;
    });
  }, []);
  return [values, setTweak];
}

function TweaksPanel({ children }) {
  // No-op shell on the live site; keeping it as a transparent wrapper
  // means app.jsx can stay identical to the prototype.
  return null;
}

function TweakSection() { return null; }
function TweakRadio() { return null; }
function TweakToggle() { return null; }
function TweakSlider() { return null; }
function TweakColor() { return null; }
function TweakSelect() { return null; }
function TweakText() { return null; }
function TweakNumber() { return null; }
function TweakButton() { return null; }
function TweakRow() { return null; }

Object.assign(window, {
  useTweaks, TweaksPanel, TweakSection, TweakRow,
  TweakSlider, TweakToggle, TweakRadio, TweakSelect,
  TweakText, TweakNumber, TweakColor, TweakButton,
});
