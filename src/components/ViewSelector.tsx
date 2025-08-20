"use client";

export type ViewType = "city" | "community" | "stations";

interface ViewSelectorProps {
  selectedView: ViewType;
  onViewChange: (view: ViewType) => void;
}

export default function ViewSelector({ selectedView, onViewChange }: ViewSelectorProps) {
  const views = [
    { id: "city" as ViewType, label: "City-wide", icon: "🏙️" },
    { id: "community" as ViewType, label: "Community Areas", icon: "🏘️" },
    { id: "stations" as ViewType, label: "Stations", icon: "🚲" }
  ];

  return (
    <div className="flex flex-col sm:flex-row gap-2 mb-6">
      <span className="text-sm font-medium text-gray-700 self-center mb-2 sm:mb-0">
        View by:
      </span>
      <div className="flex bg-gray-100 rounded-lg p-1">
        {views.map((view) => (
          <button
            key={view.id}
            onClick={() => onViewChange(view.id)}
            className={`
              flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors
              ${selectedView === view.id
                ? "bg-white text-blue-600 shadow-sm"
                : "text-gray-600 hover:text-gray-900"
              }
            `}
          >
            <span>{view.icon}</span>
            {view.label}
          </button>
        ))}
      </div>
    </div>
  );
}